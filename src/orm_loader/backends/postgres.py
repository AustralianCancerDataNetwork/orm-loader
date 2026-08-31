from __future__ import annotations

from contextlib import AbstractContextManager, contextmanager
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
import sqlalchemy.event as sae
import sqlalchemy.orm as so
from oa_configurator import qualified, schema_of
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql.compiler import IdentifierPreparer
from sqlalchemy.sql.selectable import SelectBase

from .base import BackendCapabilities, DatabaseBackend, Dialect
from .materialized_view_errors import (
    ConcurrentRefreshNotEligibleError,
    MaterializationError,
    MaterializationFailure,
    MaterializationOperation,
    MaterializationOutcome,
    UnsupportedMaterializationDialectError,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine

    from ..loaders.data_classes import LoaderContext
    from ..mappers.materialised_view_contracts import MaterializedViewIndex
    from ..tables.typing import CSVTableProtocol

_VALID_PG_REPLICATION_ROLES = frozenset({"origin", "local", "replica"})


def _require_postgres_dialect(
    conn: "Connection",
    *,
    operation: MaterializationOperation,
    schema: str | None,
    name: str,
) -> None:
    dialect = getattr(conn, "dialect", None)
    if dialect is not None and dialect.name == "postgresql":
        return
    raise UnsupportedMaterializationDialectError(
        MaterializationFailure(
            operation=operation,
            schema=schema,
            name=name,
            reason=f"received dialect {getattr(dialect, 'name', dialect)!r}",
        )
    )


def _outcome(
    operation: MaterializationOperation,
    *,
    schema: str | None,
    name: str,
    index_name: str | None = None,
) -> MaterializationOutcome:
    return MaterializationOutcome(
        operation=operation,
        schema=schema,
        name=name,
        index_name=index_name,
    )


def _error(
    error: Exception,
    operation: MaterializationOperation,
    *,
    schema: str | None,
    name: str,
    index_name: str | None = None,
) -> MaterializationError:
    return MaterializationError(
        MaterializationFailure(
            operation=operation,
            schema=schema,
            name=name,
            index_name=index_name,
            reason=str(error),
            cause=error,
        )
    )


class PostgresBackend(DatabaseBackend):
    def __init__(self, *, staging_schema: str | None = None) -> None:
        super().__init__(staging_schema=staging_schema)

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

    @property
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            supports_fast_load=True,
            supports_unlogged_staging=True,
            supports_fk_toggle=True,
            supports_materialized_views=True,
        )

    def create_staging_table(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
    ) -> None:
        table = table_cls.__table__
        preparer = self.identifier_preparer
        staging_ref = self.qualified_staging_name(table_cls.__tablename__)
        source_ref = qualified(session, table.name)
        session.execute(sa.text(f'DROP TABLE IF EXISTS {staging_ref};'))
        session.execute(
            sa.text(
                f'''
                CREATE UNLOGGED TABLE {staging_ref}
                (LIKE {source_ref} INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
                '''
            )
        )

        computed_cols = [c.name for c in table.columns if c.computed is not None]
        for col in computed_cols:
            session.execute(sa.text(f'ALTER TABLE {staging_ref} DROP COLUMN {preparer.quote_identifier(col)};'))

        # allows pagination in O(N log N) time for large tables in merge_insert without needing to add an index on every staging table
        session.execute(
            sa.text(
                f'ALTER TABLE {staging_ref} ADD COLUMN _rownum BIGINT'
                f" GENERATED ALWAYS AS IDENTITY (CACHE 1000);"
            )
        )

        session.commit()

    def drop_staging_table(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
    ) -> None:
        session.execute(sa.text(f'DROP TABLE IF EXISTS {self.qualified_staging_name(table_cls.__tablename__)}'))

    def load_staging_fast(
        self,
        loader_context: "LoaderContext",
    ) -> int | None:
        from ..loaders.loading_helpers import quick_load_pg

        tablename = loader_context.tableclass.__tablename__
        return quick_load_pg(
            path=loader_context.path,
            session=loader_context.session,
            tablename=self.staging_name_for_table(tablename),
            schema=self.staging_schema,
            quote_mode=loader_context.quote_mode,
        )

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

    def disable_fk_check(self, session: so.Session) -> str | int:
        previous_state = session.execute(sa.text("SHOW session_replication_role")).scalar()
        session.execute(sa.text("SET session_replication_role = 'replica'"))
        if not isinstance(previous_state, str):
            raise RuntimeError("Expected PostgreSQL FK state to be a string")
        return previous_state

    def enable_fk_check(self, session: so.Session) -> str | int:
        previous_state = session.execute(sa.text("SHOW session_replication_role")).scalar()
        session.execute(sa.text("SET session_replication_role = 'origin'"))
        if not isinstance(previous_state, str):
            raise RuntimeError("Expected PostgreSQL FK state to be a string")
        return previous_state

    def restore_fk_check(
        self,
        session: so.Session,
        previous_state: str | int,
    ) -> None:
        safe_state = self._normalize_fk_check_state(previous_state)
        session.execute(sa.text(f"SET session_replication_role = '{safe_state}'"))

    def _staging_rownum_index(
        self, table_cls: type["CSVTableProtocol"], staging: sa.Table, session: so.Session
    ) -> None:
        staging_name = self.staging_name_for_table(table_cls.__tablename__)
        idx = sa.Index(f"{staging_name}_rownum_idx", staging.c._rownum)
        idx.create(bind=session.connection(), checkfirst=True)
        session.commit()

    def merge_replace(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
        pk_cols: list[str],
        *,
        merge_batch_size: int | None = None,
    ) -> None:
        target = table_cls.__table__
        staging = table_cls.get_staging_table(session, staging_schema=self.staging_schema)
        pk_join = sa.and_(*(target.c[c] == staging.c[c] for c in pk_cols))

        non_paginated_replace = sa.delete(target).where(pk_join)

        if merge_batch_size is None:
            session.execute(non_paginated_replace)
            return

        total = session.execute(sa.select(sa.func.count()).select_from(staging)).scalar_one()
        if total <= merge_batch_size:
            session.execute(non_paginated_replace)
            return

        self._staging_rownum_index(table_cls, staging, session)

        start = 0
        while start < total:
            end = start + merge_batch_size
            session.execute(
                sa.delete(target).where(
                    pk_join, staging.c._rownum > start, staging.c._rownum <= end
                )
            )
            session.commit()
            start = end

    def merge_upsert(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
        pk_cols: list[str],
        *,
        merge_batch_size: int | None = None,
    ) -> None:
        target = table_cls.__table__
        staging = table_cls.get_staging_table(session, staging_schema=self.staging_schema)
        insertable_cols = self._insertable_column_names(table_cls)

        def _upsert(select_: sa.sql.Select[Any]) -> sa.Insert:
            # sa.insert() has no .on_conflict_do_nothing()
            return (
                postgresql.insert(target)
                .from_select(insertable_cols, select_)
                .on_conflict_do_nothing(index_elements=pk_cols)
            )

        non_paginated_select = sa.select(*(staging.c[c] for c in insertable_cols))

        if merge_batch_size is None:
            session.execute(_upsert(non_paginated_select))
            return

        total = session.execute(sa.select(sa.func.count()).select_from(staging)).scalar_one()
        if total <= merge_batch_size:
            session.execute(_upsert(non_paginated_select))
            return

        self._staging_rownum_index(table_cls, staging, session)

        start = 0
        while start < total:
            end = start + merge_batch_size
            batch_select = non_paginated_select.where(
                staging.c._rownum > start, staging.c._rownum <= end
            )
            session.execute(_upsert(batch_select))
            session.commit()
            start = end

    def merge_insert(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
        *,
        merge_batch_size: int | None = None,
    ) -> None:
        target = table_cls.__table__
        staging = table_cls.get_staging_table(session, staging_schema=self.staging_schema)
        insertable_cols = self._insertable_column_names(table_cls)
        non_paginated_select = sa.select(*(staging.c[c] for c in insertable_cols))

        def _insert(select_: sa.sql.Select[Any]) -> sa.Insert:
            return sa.insert(target).from_select(insertable_cols, select_)

        if merge_batch_size is None:
            session.execute(_insert(non_paginated_select))
            return

        total = session.execute(sa.select(sa.func.count()).select_from(staging)).scalar_one()
        if total <= merge_batch_size:
            session.execute(_insert(non_paginated_select))
            return

        # Paginated path: index _rownum for O(N log N) range scans then
        # INSERT in batch-sized transactions to bound WAL per commit.
        # session_replication_role='replica' is session-level and persists
        # across commits, so FK checks stay disabled for all batches.
        self._staging_rownum_index(table_cls, staging, session)

        start = 0
        while start < total:
            end = start + merge_batch_size
            batch_select = non_paginated_select.where(
                staging.c._rownum > start, staging.c._rownum <= end
            )
            session.execute(_insert(batch_select))
            session.commit()
            start = end

    def merge_context(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
    ) -> AbstractContextManager[None]:
        return self.bulk_load_context(session, disable_fk=True, no_autoflush=False)

    def create_materialized_view(
        self,
        bind: Engine | Connection,
        name: str,
        selectable: SelectBase,
        *,
        schema: str | None = None,
        with_data: bool = True,
        if_not_exists: bool = False,
    ) -> MaterializationOutcome:
        from ..mappers.materialised_view_mixin import CreateMaterializedView

        with self._as_connection(bind) as conn:
            effective_schema = schema if schema is not None else schema_of(conn)
            _require_postgres_dialect(
                conn,
                operation=MaterializationOperation.CREATE,
                schema=effective_schema,
                name=name,
            )
            try:
                conn.execute(
                    CreateMaterializedView(
                        name,
                        selectable,
                        schema=effective_schema,
                        with_data=with_data,
                        if_not_exists=if_not_exists,
                    )
                )
            except Exception as error:
                raise _error(
                    error,
                    MaterializationOperation.CREATE,
                    schema=effective_schema,
                    name=name,
                ) from error
            return _outcome(
                MaterializationOperation.CREATE,
                schema=effective_schema,
                name=name,
            )

    def refresh_materialized_view(
        self,
        bind: Engine | Connection,
        name: str,
        *,
        schema: str | None = None,
        concurrently: bool = False,
        declared_indexes: tuple["MaterializedViewIndex", ...] = (),
    ) -> MaterializationOutcome:
        with self._as_connection(bind) as conn:
            effective_schema = schema if schema is not None else schema_of(conn)
            _require_postgres_dialect(
                conn,
                operation=MaterializationOperation.REFRESH,
                schema=effective_schema,
                name=name,
            )

            if concurrently:
                # Cheap, in-memory check only: did the caller declare a unique
                # index at all. Whether that index genuinely exists and is
                # valid in this database is PostgreSQL's own prerequisite for
                # CONCURRENTLY, checked below by attempting the refresh and
                # translating its rejection rather than re-deriving the same
                # rule ourselves against the live catalog.
                if not any(index.unique for index in declared_indexes):
                    raise ConcurrentRefreshNotEligibleError(
                        MaterializationFailure(
                            operation=MaterializationOperation.REFRESH,
                            schema=effective_schema,
                            name=name,
                            reason="no simple unique index is declared",
                        )
                    )

            safe_name = qualified(conn, name, schema=effective_schema)
            concurrency = "CONCURRENTLY " if concurrently else ""
            try:
                conn.execute(sa.text(f"REFRESH MATERIALIZED VIEW {concurrency}{safe_name};"))
            except OperationalError as error:
                # Deferred: this backend must not force a hard psycopg
                # dependency at import time, only when actually handling a
                # real Postgres failure (i.e. psycopg is necessarily already
                # in use as this project's one supported Postgres driver).
                from psycopg.errors import ObjectNotInPrerequisiteState

                if concurrently and isinstance(error.orig, ObjectNotInPrerequisiteState):
                    raise ConcurrentRefreshNotEligibleError(
                        MaterializationFailure(
                            operation=MaterializationOperation.REFRESH,
                            schema=effective_schema,
                            name=name,
                            reason=str(error.orig).strip(),
                            cause=error,
                        )
                    ) from error
                raise _error(
                    error,
                    MaterializationOperation.REFRESH,
                    schema=effective_schema,
                    name=name,
                ) from error
            except Exception as error:
                raise _error(
                    error,
                    MaterializationOperation.REFRESH,
                    schema=effective_schema,
                    name=name,
                ) from error
            return _outcome(
                MaterializationOperation.REFRESH,
                schema=effective_schema,
                name=name,
            )

    def drop_materialized_view(
        self,
        bind: Engine | Connection,
        name: str,
        *,
        schema: str | None = None,
        if_exists: bool = True,
        cascade: bool = False,
    ) -> MaterializationOutcome:
        from ..mappers.materialised_view_contracts import DropMaterializedView

        with self._as_connection(bind) as conn:
            effective_schema = schema if schema is not None else schema_of(conn)
            _require_postgres_dialect(
                conn,
                operation=MaterializationOperation.DROP,
                schema=effective_schema,
                name=name,
            )
            try:
                conn.execute(
                    DropMaterializedView(
                        name,
                        schema=effective_schema,
                        if_exists=if_exists,
                        cascade=cascade,
                    )
                )
            except Exception as error:
                raise _error(
                    error,
                    MaterializationOperation.DROP,
                    schema=effective_schema,
                    name=name,
                ) from error
            return _outcome(
                MaterializationOperation.DROP,
                schema=effective_schema,
                name=name,
            )

    def create_materialized_view_index(
        self,
        bind: Engine | Connection,
        name: str,
        index: "MaterializedViewIndex",
        *,
        schema: str | None = None,
        if_not_exists: bool = False,
    ) -> MaterializationOutcome:
        from ..mappers.materialised_view_contracts import CreateMaterializedViewIndex

        with self._as_connection(bind) as conn:
            effective_schema = schema if schema is not None else schema_of(conn)
            _require_postgres_dialect(
                conn,
                operation=MaterializationOperation.CREATE_INDEX,
                schema=effective_schema,
                name=name,
            )
            try:
                conn.execute(
                    CreateMaterializedViewIndex(
                        name,
                        index,
                        schema=effective_schema,
                        if_not_exists=if_not_exists,
                    )
                )
            except Exception as error:
                raise _error(
                    error,
                    MaterializationOperation.CREATE_INDEX,
                    schema=effective_schema,
                    name=name,
                    index_name=index.name,
                ) from error
            return _outcome(
                MaterializationOperation.CREATE_INDEX,
                schema=effective_schema,
                name=name,
                index_name=index.name,
            )

    @contextmanager
    def engine_with_replica_role(self, engine: "Engine"):
        def _set_replica_role(
            dbapi_conn: sa.engine.interfaces.DBAPIConnection,
            _,
        ) -> None:
            cur = dbapi_conn.cursor()
            cur.execute("SET session_replication_role = 'replica'")
            cur.close()

        sae.listen(engine, "connect", _set_replica_role)

        try:
            yield engine
        finally:
            sae.remove(engine, "connect", _set_replica_role)
            with engine.connect() as conn:
                conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                conn.execute(sa.text("SET session_replication_role = DEFAULT"))
                role = conn.execute(sa.text("SHOW session_replication_role")).scalar()
                if role != "origin":
                    raise RuntimeError("Failed to restore session_replication_role")
