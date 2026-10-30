from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import logging
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.compute as pc
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy import text
from sqlalchemy.dialects import sqlite as sqlite_dialect
from sqlalchemy.sql.compiler import IdentifierPreparer

from .base import DatabaseBackend, Dialect

if TYPE_CHECKING:
    from oa_configurator import ResolvedDatabase

    from ..loaders.loader import LoaderContext
    from ..tables.typing import FileTableProtocol


logger = logging.getLogger(__name__)


class SQLiteBackend(DatabaseBackend):
    @staticmethod
    def staging_name_for_table(tablename: str) -> str:
        return f"_staging_{tablename}"

    def __init__(
        self,
        *,
        staging_schema_tag: str | None = None,
        staging_schema: str | None = None,
        resolved: ResolvedDatabase | None = None,
        physical_schemas: Mapping[str, str | None] | None = None,
    ) -> None:
        if staging_schema is not None:
            raise ValueError(
                "SQLite has no schema concept, so staging_schema must be None. "
                "physical_schema_of() already resolves it to None for this dialect."
            )
        super().__init__(
            staging_schema_tag=staging_schema_tag,
            staging_schema=staging_schema,
            resolved=resolved,
            physical_schemas=physical_schemas,
        )

    @staticmethod
    def _normalize_fk_check_state(previous_state: str | int) -> str:
        if isinstance(previous_state, int):
            if previous_state == 1:
                return "ON"
            if previous_state == 0:
                return "OFF"
        elif isinstance(previous_state, str):
            normalised = previous_state.strip().upper()
            if normalised in {"1", "ON"}:
                return "ON"
            if normalised in {"0", "OFF"}:
                return "OFF"
        raise ValueError(
            f"Invalid SQLite foreign_keys state {previous_state!r}. "
            "Expected 0, 1, 'OFF', or 'ON'."
        )

    @property
    def name(self) -> str:
        return Dialect.SQLITE

    @property
    def dialect(self) -> Dialect:
        return Dialect.SQLITE

    @property
    def identifier_preparer(self) -> IdentifierPreparer:
        return sqlite_dialect.dialect().identifier_preparer

    @property
    def default_index_strategy(self) -> str:
        return "keep"

    def insert_batches(
        self,
        ctx: "LoaderContext",
        staging: sa.Table,
        batches: Iterable[pa.Table],
        columns: Sequence[str],
    ) -> None:
        """Insert the batches with one native ``executemany`` per batch.

        The statement goes straight to the DB-API without per-value bind
        processing, so the connection's schema translation is applied here.
        Dates, timestamps and decimals are passed as the text SQLAlchemy
        itself stores for them.
        """
        connection = ctx.session.connection(bind_arguments={"mapper": ctx.tableclass})
        preparer = connection.dialect.identifier_preparer
        table_ref = self._staging_reference(staging, connection)
        sql = (
            f"INSERT INTO {table_ref} ({', '.join(preparer.quote(c) for c in columns)}) "
            f"VALUES ({', '.join('?' for _ in columns)})"
        )
        for table in batches:
            values = [self._storable(table[name]) for name in columns]
            connection.exec_driver_sql(sql, list(zip(*values)))

    @staticmethod
    def _storable(column: pa.ChunkedArray) -> list[Any]:
        """Return a column as Python values SQLite can bind."""
        kind = column.type
        if pa.types.is_date(kind) or pa.types.is_timestamp(kind) or pa.types.is_decimal(kind):
            column = pc.cast(column, pa.string())
        return column.to_pylist()

    def evaluate_default(
        self,
        session: so.Session,
        table_cls: type["FileTableProtocol"],
        expression: sa.ColumnElement[Any],
        column_type: Any,
    ) -> Any:
        """Evaluate without a cast: SQLite casts a date to the integer of its year."""
        return session.execute(sa.select(expression), bind_arguments={"mapper": table_cls}).scalar_one()

    def disable_fk_check(self, session: so.Session, *, mapper: Any | None = None) -> str | int | None:
        """Turn ``PRAGMA foreign_keys`` off and return its previous value.

        SQLite ignores the pragma inside an open transaction, so the state is
        read back and a pragma that did not take effect raises.

        Raises
        ------
        RuntimeError
            The connection kept enforcing foreign keys.
        """
        execute_options = {"bind_arguments": {"mapper": mapper}} if mapper is not None else {}
        previous_state = session.execute(text("PRAGMA foreign_keys"), **execute_options).scalar()
        if not isinstance(previous_state, int):
            raise RuntimeError("Expected SQLite FK state to be an int")
        session.execute(text("PRAGMA foreign_keys = OFF"), **execute_options)
        if session.execute(text("PRAGMA foreign_keys"), **execute_options).scalar():
            raise RuntimeError(
                "SQLite ignored PRAGMA foreign_keys = OFF because the connection is inside a transaction. "
                "Commit the session's pending writes before loading."
            )
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
        session.execute(text(f"PRAGMA foreign_keys = {safe_state}"), **execute_options)

    def _pagination_column(
        self, table_cls: type["FileTableProtocol"], staging: sa.Table
    ) -> sa.ColumnElement[int]:
        """SQLite's implicit rowid is already gapless and indexed."""
        return sa.literal_column("rowid")

    def _ensure_pagination_index(
        self, table_cls: type["FileTableProtocol"], staging: sa.Table, session: so.Session
    ) -> None:
        """No-op: rowid needs no index, and the staging table has no _rownum column."""
        return

    @property
    def _commits_between_pagination_batches(self) -> bool:
        """False: keeps the whole merge in the caller's transaction, so a
        later insert-phase failure can't leave a delete-then-reinsert merge
        with the target emptied and nothing to replace it.
        """
        return False

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
        """Delete every target row whose primary key appears in staging.

        Notes
        -----
        validate_deferred_fks is accepted for interface parity only:
        _commits_between_pagination_batches is always False here, so no
        intermediate commit, and nothing to validate ahead of, ever happens.
        """
        self._validate_merge_batch_size(merge_batch_size)
        target = table_cls.__table__

        # SQLite's DELETE has no USING, so staging keys go in a subquery. IN
        # rather than a correlated EXISTS, which rescans the unindexed staging
        # table once per target row.
        def _delete(extra: sa.ColumnElement[bool] | None = None) -> sa.Delete:
            keys = sa.select(*(staging.c[c] for c in pk_cols))
            if extra is not None:
                keys = keys.where(extra)
            if len(pk_cols) == 1:
                return sa.delete(target).where(target.c[pk_cols[0]].in_(keys))
            return sa.delete(target).where(
                sa.tuple_(*(target.c[c] for c in pk_cols)).in_(keys)
            )

        for batch_range in self._paginate_batches(table_cls, staging, session, merge_batch_size):
            session.execute(_delete(batch_range))

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
            return (
                sqlite_dialect.insert(target)
                .from_select(insertable_cols, select_)
                .on_conflict_do_nothing(index_elements=pk_cols)
            )

        non_paginated_select = sa.select(*(staging.c[c] for c in insertable_cols))

        for batch_range in self._paginate_batches(table_cls, staging, session, merge_batch_size):

            select_ = non_paginated_select.where(batch_range if batch_range is not None else sa.true())
            session.execute(_upsert(select_))

    def merge_insert(
        self,
        table_cls: type["FileTableProtocol"],
        session: so.Session,
        staging: sa.Table,
        *,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ) -> None:
        self._validate_merge_batch_size(merge_batch_size)
        target = table_cls.__table__
        insertable_cols = self._insertable_column_names(table_cls, staging)
        non_paginated_select = sa.select(*(staging.c[c] for c in insertable_cols))

        def _insert(select_: Any) -> sa.Insert:
            return sa.insert(target).from_select(insertable_cols, select_)

        for batch_range in self._paginate_batches(table_cls, staging, session, merge_batch_size):
            select_ = non_paginated_select if batch_range is None else non_paginated_select.where(batch_range)
            session.execute(_insert(select_))
