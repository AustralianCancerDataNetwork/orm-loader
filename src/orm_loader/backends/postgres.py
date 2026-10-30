from __future__ import annotations

import io
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.csv as pacsv
import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import (
    Dialect,
    qualified,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql.compiler import IdentifierPreparer

from ..mappers.materialised_view_mixin import MaterializedViewBackend
from ..mappers.materialised_view_errors import (
    ConcurrentRefreshNotEligibleError,
    MaterializationError,
    MaterializationFailure,
    MaterializationOperation,
)
from .base import DatabaseBackend

if TYPE_CHECKING:
    from oa_configurator import ResolvedDatabase
    from sqlalchemy.engine import Connection, Engine

    from ..loaders.loader import LoaderContext
    from ..mappers.materialised_view_contracts import MaterializedViewIndex
    from ..tables.typing import FileTableProtocol

_VALID_PG_REPLICATION_ROLES = frozenset({"origin", "local", "replica"})


class PostgresBackend(DatabaseBackend, MaterializedViewBackend):
    def __init__(
        self,
        *,
        staging_schema_tag: str | None = None,
        staging_schema: str | None = None,
        resolved: ResolvedDatabase | None = None,
        physical_schemas: Mapping[str, str | None] | None = None,
    ) -> None:
        super().__init__(
            staging_schema_tag=staging_schema_tag,
            staging_schema=staging_schema,
            resolved=resolved,
            physical_schemas=physical_schemas,
        )

    @staticmethod
    def staging_name_for_table(tablename: str) -> str:
        return f"_staging_{tablename}"

    @property
    def name(self) -> str:
        return "postgres"

    @property
    def dialect(self) -> Dialect:
        return Dialect.POSTGRESQL

    @property
    def identifier_preparer(self) -> IdentifierPreparer:
        return postgresql.dialect().identifier_preparer

    def _staging_table(self, table_cls: type["FileTableProtocol"], columns: Sequence[sa.Column[Any]]) -> sa.Table:
        """Unlogged, with an identity column, which paginates the merge in O(N log N) without an index."""
        return sa.Table(
            self.staging_name_for_table(table_cls.__tablename__),
            sa.MetaData(),
            *columns,
            sa.Column("_rownum", sa.BigInteger, sa.Identity(always=True, cache=1000)),
            schema=self.staging_schema,
            prefixes=["UNLOGGED"],
        )

    def insert_batches(
        self,
        ctx: "LoaderContext",
        staging: sa.Table,
        batches: Iterable[pa.Table],
        columns: Sequence[str],
    ) -> None:
        """Stream the batches into the staging table with one COPY, as CSV written by Arrow.

        An unquoted empty field is NULL and a quoted one an empty string, so
        the null policy's decisions reach the table unchanged. A failure in
        any batch aborts the COPY.
        """
        connection = ctx.session.connection(bind_arguments={"mapper": ctx.tableclass})
        cursor = connection.connection.cursor()
        if not hasattr(cursor, "copy"):
            cursor.close()
            raise RuntimeError("The PostgreSQL sink needs a psycopg 3 connection, whose cursors provide copy()")
        table_ref = self._staging_reference(staging, connection)
        column_list = ", ".join(self.identifier_preparer.quote(c) for c in columns)
        options = pacsv.WriteOptions(include_header=False, quoting_style="needed")
        try:
            with cursor.copy(f"COPY {table_ref} ({column_list}) FROM STDIN WITH (FORMAT csv)") as copy:
                for table in batches:
                    buffer = io.BytesIO()
                    pacsv.write_csv(table, buffer, options)
                    copy.write(buffer.getvalue())
            ctx.session.flush()
        finally:
            cursor.close()

    @staticmethod
    def _normalize_fk_check_state(previous_state: str | int) -> str:
        if isinstance(previous_state, int):
            raise ValueError(
                f"Invalid PostgreSQL session_replication_role {previous_state!r}: "
                "Postgres uses string roles ('origin', 'local', 'replica'), not integers. "
                "The value passed here should always come from this backend's own "
                "disable_fk_check(), which returns a string."
            )
        normalised = previous_state.strip().lower()
        if normalised not in _VALID_PG_REPLICATION_ROLES:
            raise ValueError(
                f"Invalid PostgreSQL session_replication_role {previous_state!r}. "
                f"Expected one of: {sorted(_VALID_PG_REPLICATION_ROLES)}"
            )
        return normalised

    def disable_fk_check(self, session: so.Session, *, mapper: Any | None = None) -> str:
        execute_options = {"bind_arguments": {"mapper": mapper}} if mapper is not None else {}
        previous_state = session.execute(
            sa.text("SHOW session_replication_role"), **execute_options
        ).scalar()
        session.execute(sa.text("SET session_replication_role = 'replica'"), **execute_options)
        if not isinstance(previous_state, str):
            raise RuntimeError("Expected PostgreSQL FK state to be a string")
        return previous_state

    def restore_fk_check(
        self,
        session: so.Session,
        previous_state: str | int,
        *,
        mapper: Any | None = None,
    ) -> None:
        safe_state = self._normalize_fk_check_state(previous_state)
        execute_options = {"bind_arguments": {"mapper": mapper}} if mapper is not None else {}
        session.execute(
            sa.text(f"SET session_replication_role = '{safe_state}'"), **execute_options
        )

    @property
    def _commits_between_pagination_batches(self) -> bool:
        """True: bounds the WAL growth, lock duration, and dead-tuple
        accumulation of one uncommitted transaction over a large table.

        A crash between a delete batch and its insert batch leaves those
        rows missing until the load runs again. The next run deletes them
        from the target again (their primary keys are still in the freshly
        reloaded staging table) before reinserting, so no manual cleanup
        is needed, just a rerun.
        """
        return True

    def merge_replace(
        self,
        table_cls: type["FileTableProtocol"],
        session: so.Session,
        pk_cols: list[str],
        staging: sa.Table,
        *,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ) -> None:
        """Delete every target row whose primary key appears in staging."""
        self._validate_merge_batch_size(merge_batch_size)
        target = table_cls.__table__
        pk_join = sa.and_(*(target.c[c] == staging.c[c] for c in pk_cols))

        for batch_range in self._paginate_batches(table_cls, staging, session, merge_batch_size):
            where = pk_join if batch_range is None else sa.and_(pk_join, batch_range)
            session.execute(sa.delete(target).where(where))
            self._commit_batch_if_needed(
                table_cls, session, batch_range, validate_deferred_fks=validate_deferred_fks
            )

    def merge_upsert(
        self,
        table_cls: type["FileTableProtocol"],
        session: so.Session,
        pk_cols: list[str],
        staging: sa.Table,
        *,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ) -> None:
        self._validate_merge_batch_size(merge_batch_size)
        target = table_cls.__table__
        insertable_cols = self._insertable_column_names(table_cls, staging)

        def _upsert(select_: Any) -> sa.Insert:
            # sa.insert() has no .on_conflict_do_nothing()
            return (
                postgresql.insert(target)
                .from_select(insertable_cols, select_)
                .on_conflict_do_nothing(index_elements=pk_cols)
            )

        non_paginated_select = sa.select(*(staging.c[c] for c in insertable_cols))

        for batch_range in self._paginate_batches(table_cls, staging, session, merge_batch_size):
            select_ = non_paginated_select if batch_range is None else non_paginated_select.where(batch_range)
            session.execute(_upsert(select_))
            self._commit_batch_if_needed(
                table_cls, session, batch_range, validate_deferred_fks=validate_deferred_fks
            )

    def merge_insert(
        self,
        table_cls: type["FileTableProtocol"],
        session: so.Session,
        staging: sa.Table,
        *,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ) -> None:
        """Insert all staging rows into the target table.

        session_replication_role='replica' is session-level and persists
        across commits, so FK checks stay disabled for every batch.
        """
        self._validate_merge_batch_size(merge_batch_size)
        target = table_cls.__table__
        insertable_cols = self._insertable_column_names(table_cls, staging)
        non_paginated_select = sa.select(*(staging.c[c] for c in insertable_cols))

        def _insert(select_: Any) -> sa.Insert:
            return sa.insert(target).from_select(insertable_cols, select_)

        for batch_range in self._paginate_batches(table_cls, staging, session, merge_batch_size):
            select_ = non_paginated_select if batch_range is None else non_paginated_select.where(batch_range)
            session.execute(_insert(select_))
            self._commit_batch_if_needed(
                table_cls, session, batch_range, validate_deferred_fks=validate_deferred_fks
            )

    def create_materialized_view(
        self,
        bind: Engine | Connection,
        name: str,
        selectable: sa.sql.Select[Any],
        *,
        schema: str | None = None,
        with_data: bool = True,
        if_not_exists: bool = True,
    ) -> None:
        from ..mappers.materialised_view_mixin import CreateMaterializedView

        with self._as_connection(bind) as conn:
            try:
                conn.execute(
                    CreateMaterializedView(
                        qualified(conn, name, physical_schema=schema),
                        selectable,
                        with_data=with_data,
                        if_not_exists=if_not_exists,
                    )
                )
            except Exception as error:
                raise MaterializationError(
                    MaterializationFailure(
                        operation=MaterializationOperation.CREATE,
                        schema=schema,
                        name=name,
                        reason=str(error),
                        cause=error,
                    )
                ) from error

    def refresh_materialized_view(
        self,
        bind: Engine | Connection,
        name: str,
        *,
        schema: str | None = None,
        concurrently: bool = False,
        declared_indexes: tuple["MaterializedViewIndex", ...] = (),
    ) -> None:
        with self._as_connection(bind) as conn:
            if concurrently:
                if not any(index.unique for index in declared_indexes):
                    raise ConcurrentRefreshNotEligibleError(
                        MaterializationFailure(
                            operation=MaterializationOperation.REFRESH,
                            schema=schema,
                            name=name,
                            reason="no simple unique index is declared",
                        )
                    )

            safe_name = qualified(conn, name, physical_schema=schema)
            concurrency = "CONCURRENTLY " if concurrently else ""
            try:
                conn.execute(sa.text(f"REFRESH MATERIALIZED VIEW {concurrency}{safe_name};"))
            except OperationalError as error:
                from psycopg.errors import ObjectNotInPrerequisiteState

                if not concurrently or not isinstance(error.orig, ObjectNotInPrerequisiteState):
                    raise
                raise ConcurrentRefreshNotEligibleError(
                    MaterializationFailure(
                        operation=MaterializationOperation.REFRESH,
                        schema=schema,
                        name=name,
                        reason=str(error.orig).strip(),
                        cause=error,
                    )
                ) from error

    def drop_materialized_view(
        self,
        bind: Engine | Connection,
        name: str,
        *,
        schema: str | None = None,
        if_exists: bool = True,
        cascade: bool = False,
    ) -> None:
        from ..mappers.materialised_view_contracts import DropMaterializedView

        with self._as_connection(bind) as conn:
            try:
                conn.execute(
                    DropMaterializedView(
                        qualified(conn, name, physical_schema=schema), if_exists=if_exists, cascade=cascade
                    )
                )
            except Exception as error:
                raise MaterializationError(
                    MaterializationFailure(
                        operation=MaterializationOperation.DROP,
                        schema=schema,
                        name=name,
                        reason=str(error),
                        cause=error,
                    )
                ) from error

    def create_materialized_view_index(
        self,
        bind: Engine | Connection,
        name: str,
        index: "MaterializedViewIndex",
        *,
        schema: str | None = None,
        if_not_exists: bool = True,
    ) -> None:
        from ..mappers.materialised_view_contracts import CreateMaterializedViewIndex

        with self._as_connection(bind) as conn:
            try:
                conn.execute(
                    CreateMaterializedViewIndex(
                        qualified(conn, name, physical_schema=schema), index, if_not_exists=if_not_exists
                    )
                )
            except Exception as error:
                raise MaterializationError(
                    MaterializationFailure(
                        operation=MaterializationOperation.CREATE_INDEX,
                        schema=schema,
                        name=name,
                        index_name=index.name,
                        reason=str(error),
                        cause=error,
                    )
                ) from error
