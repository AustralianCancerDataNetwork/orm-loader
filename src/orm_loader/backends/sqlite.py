from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import logging
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.compute as pc
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy import event, text
from sqlalchemy.dialects import sqlite as sqlite_dialect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import DropTable
from sqlalchemy.sql.compiler import IdentifierPreparer

from .base import BackendCapabilities, DatabaseBackend, Dialect

if TYPE_CHECKING:
    from oa_configurator import ResolvedDatabase
    from sqlalchemy.engine import Connection, Engine

    from ..loaders.loader import LoaderContext
    from ..tables.typing import FileTableProtocol


logger = logging.getLogger(__name__)
VALID_SQLITE_JOURNAL_MODES = frozenset(
    {"DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF"}
)


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
        busy_timeout_ms: int = 60000,
        journal_mode: str = "WAL",
        defer_foreign_keys: bool = True,
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
        self.busy_timeout_ms = busy_timeout_ms
        self.journal_mode = self._validate_journal_mode(journal_mode)
        self.defer_foreign_keys = defer_foreign_keys

    @staticmethod
    def _validate_journal_mode(journal_mode: str) -> str:
        normalised = journal_mode.strip().upper()
        if normalised not in VALID_SQLITE_JOURNAL_MODES:
            raise ValueError(
                "Unsupported SQLite journal_mode "
                f"{journal_mode!r}. Expected one of: {sorted(VALID_SQLITE_JOURNAL_MODES)}"
            )
        return normalised

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
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            supports_unlogged_staging=False,
            supports_fk_toggle=True,
            supports_materialized_views=False,
            supports_replace_with_kept_indexes=True,
        )

    @property
    def default_index_strategy(self) -> str:
        return "keep"

    def insert_batches(
        self,
        ctx: "LoaderContext",
        batches: Iterable[pa.Table],
        columns: Sequence[str],
    ) -> int:
        """Insert the batches with one native ``executemany`` per batch.

        The statement goes straight to the DB-API without per-value bind
        processing, so the connection's schema translation is applied here.
        Dates, timestamps and decimals are passed as the text SQLAlchemy
        itself stores for them.
        """
        connection = ctx.session.connection(bind_arguments={"mapper": ctx.tableclass})
        preparer = connection.dialect.identifier_preparer
        table_ref = self._staging_reference(ctx, connection)
        sql = (
            f"INSERT INTO {table_ref} ({', '.join(preparer.quote(c) for c in columns)}) "
            f"VALUES ({', '.join('?' for _ in columns)})"
        )
        total = 0
        for table in batches:
            values = [self._storable(table[name]) for name in columns]
            connection.exec_driver_sql(sql, list(zip(*values)))
            total += table.num_rows
        return total

    @staticmethod
    def _storable(column: pa.ChunkedArray) -> list[Any]:
        """Return a column as Python values SQLite can bind."""
        kind = column.type
        if pa.types.is_date(kind) or pa.types.is_timestamp(kind) or pa.types.is_decimal(kind):
            column = pc.cast(column, pa.string())
        return column.to_pylist()

    def create_staging_table(
        self,
        table_cls: type["FileTableProtocol"],
        session: so.Session,
    ) -> None:
        staging_name = self.staging_name_for_table(table_cls.__tablename__)
        session.execute(
            DropTable(sa.Table(staging_name, sa.MetaData()), if_exists=True),
            bind_arguments={"mapper": table_cls},
        )

        metadata = sa.MetaData()
        staging_columns = [
            sa.Column(col.name, col.type, nullable=True)
            for col in table_cls.__table__.columns
        ]
        staging_table = sa.Table(
            staging_name, metadata, *staging_columns, schema=table_cls.__table__.schema
        )
        metadata.create_all(
            bind=session.connection(bind_arguments={"mapper": table_cls}), tables=[staging_table]
        )
        session.commit()

    def drop_staging_table(
        self,
        table_cls: type["FileTableProtocol"],
        session: so.Session,
    ) -> None:
        staging_name = self.staging_name_for_table(table_cls.__tablename__)
        session.execute(
            DropTable(sa.Table(staging_name, sa.MetaData()), if_exists=True),
            bind_arguments={"mapper": table_cls},
        )

    def disable_fk_check(self, session: so.Session, *, mapper: Any | None = None) -> str | int:
        execute_options = {"bind_arguments": {"mapper": mapper}} if mapper is not None else {}
        previous_state = session.execute(text("PRAGMA foreign_keys"), **execute_options).scalar()
        session.execute(text("PRAGMA foreign_keys = OFF"), **execute_options)
        if not isinstance(previous_state, int):
            raise RuntimeError("Expected SQLite FK state to be an int")
        return previous_state

    def enable_fk_check(self, session: so.Session, *, mapper: Any | None = None) -> str | int:
        execute_options = {"bind_arguments": {"mapper": mapper}} if mapper is not None else {}
        previous_state = session.execute(text("PRAGMA foreign_keys"), **execute_options).scalar()
        session.execute(text("PRAGMA foreign_keys = ON"), **execute_options)
        if not isinstance(previous_state, int):
            raise RuntimeError("Expected SQLite FK state to be an int")
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
        staging = table_cls.get_staging_table(session, staging_schema_tag=self.staging_schema_tag)

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
        *,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ) -> None:
        self._validate_merge_batch_size(merge_batch_size)
        target = table_cls.__table__
        staging = table_cls.get_staging_table(session, staging_schema_tag=self.staging_schema_tag)
        insertable_cols = self._insertable_column_names(table_cls)

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
        *,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ) -> None:
        self._validate_merge_batch_size(merge_batch_size)
        target = table_cls.__table__
        staging = table_cls.get_staging_table(session, staging_schema_tag=self.staging_schema_tag)
        insertable_cols = self._insertable_column_names(table_cls)
        non_paginated_select = sa.select(*(staging.c[c] for c in insertable_cols))

        def _insert(select_: Any) -> sa.Insert:
            return sa.insert(target).from_select(insertable_cols, select_)

        for batch_range in self._paginate_batches(table_cls, staging, session, merge_batch_size):
            select_ = non_paginated_select if batch_range is None else non_paginated_select.where(batch_range)
            session.execute(_insert(select_))

    def configure_dbapi_connection(self, dbapi_connection:  sa.engine.interfaces.DBAPIConnection) -> None:
        if dbapi_connection.__class__.__module__.startswith("sqlite3"):
            cursor = dbapi_connection.cursor()
            cursor.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            cursor.execute(f"PRAGMA journal_mode = {self.journal_mode}")
            cursor.execute("PRAGMA foreign_keys = ON;")
            if self.defer_foreign_keys:
                cursor.execute("PRAGMA defer_foreign_keys = ON;")
            cursor.close()

    def install_engine_hooks(self, engine: "Engine") -> None:
        @event.listens_for(engine, "connect")
        def _enable_sqlite_foreign_keys(
            dbapi_connection: sa.engine.interfaces.DBAPIConnection, 
            _connection_record: Any
        ) -> None:
            self.configure_dbapi_connection(dbapi_connection)

    def explain_fk_error(
        self,
        session: so.Session,
        exc: IntegrityError,
        *,
        mapper: Any | None = None,
        raise_error: bool = True,
    ) -> None:
        bind: Engine | Connection = session.get_bind(mapper=mapper)
        if bind.dialect.name != Dialect.SQLITE:
            raise exc

        with self._as_connection(bind) as conn:
            rows = conn.execute(text("PRAGMA foreign_key_check")).fetchall()

        if rows:
            for row in rows:
                logger.error(
                    "FK violation: table=%s rowid=%s references=%s fk_index=%s",
                    row[0], row[1], row[2], row[3]
                )

        if raise_error:
            raise exc

    def restore_journal_mode(self, db_path: Path) -> None:
        timeout_s = max(self.busy_timeout_ms / 1000, 5)
        try:
            with sqlite3.connect(db_path.resolve(), timeout=timeout_s) as conn:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                conn.execute("PRAGMA journal_mode = DELETE")
                conn.commit()
        except sqlite3.OperationalError as exc:
            raise RuntimeError(
                "Failed to restore SQLite journal mode. "
                "Close or dispose active SQLite connections before calling this helper."
            ) from exc
