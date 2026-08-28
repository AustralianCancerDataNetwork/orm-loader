from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Any
from contextlib import AbstractContextManager

import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy import event, text
from sqlalchemy.dialects import sqlite as sqlite_dialect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.compiler import IdentifierPreparer

from .base import BackendCapabilities, DatabaseBackend, Dialect

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine

    from ..tables.typing import CSVTableProtocol


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
        staging_schema: str | None = None,
        busy_timeout_ms: int = 60000,
        journal_mode: str = "WAL",
        defer_foreign_keys: bool = True,
    ) -> None:
        if staging_schema is not None:
            logger.warning(
                "SQLite does not support schema-qualified staging tables; "
                f"got staging_schema={staging_schema!r}. Setting staging_schema=None."
            )
            staging_schema = None
        super().__init__(staging_schema=staging_schema)
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
        return "sqlite"

    @property
    def dialect(self) -> Dialect:
        return Dialect.SQLITE

    @property
    def identifier_preparer(self) -> IdentifierPreparer:
        return sqlite_dialect.dialect().identifier_preparer

    @property
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            supports_fast_load=False,
            supports_unlogged_staging=False,
            supports_fk_toggle=True,
            supports_materialized_views=False,
        )

    @property
    def default_index_strategy(self) -> str:
        return "keep"

    def create_staging_table(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
    ) -> None:
        staging_name = self.staging_name_for_table(table_cls.__tablename__)
        session.execute(sa.text(f'DROP TABLE IF EXISTS {self.identifier_preparer.quote_identifier(staging_name)};'))

        metadata = sa.MetaData()
        staging_columns = [
            sa.Column(col.name, col.type, nullable=True)
            for col in table_cls.__table__.columns
        ]
        staging_table = sa.Table(staging_name, metadata, *staging_columns)
        metadata.create_all(bind=session.connection(), tables=[staging_table])
        session.commit()

    def drop_staging_table(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
    ) -> None:
        staging_ref = self.identifier_preparer.quote_identifier(self.staging_name_for_table(table_cls.__tablename__))
        session.execute(sa.text(f'DROP TABLE IF EXISTS {staging_ref}'))

    def disable_fk_check(self, session: so.Session) -> str | int:
        previous_state = session.execute(text("PRAGMA foreign_keys")).scalar()
        session.execute(text("PRAGMA foreign_keys = OFF"))
        if not isinstance(previous_state, int):
            raise RuntimeError("Expected SQLite FK state to be an int")
        return previous_state

    def enable_fk_check(self, session: so.Session) -> str | int:
        previous_state = session.execute(text("PRAGMA foreign_keys")).scalar()
        session.execute(text("PRAGMA foreign_keys = ON"))
        if not isinstance(previous_state, int):
            raise RuntimeError("Expected SQLite FK state to be an int")
        return previous_state

    def restore_fk_check(
        self,
        session: so.Session,
        previous_state: str | int,
    ) -> None:
        safe_state = self._normalize_fk_check_state(previous_state)
        session.execute(text(f"PRAGMA foreign_keys = {safe_state}"))

    @staticmethod
    def _staging_rowid() -> sa.ColumnElement[int]:
        """SQLite's implicit rowid: already gapless and indexed, so it needs
        no added column or index the way Postgres's _rownum does."""
        return sa.literal_column("rowid")

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
        pk_match = sa.and_(*(target.c[c] == staging.c[c] for c in pk_cols))

        # SQLite's DELETE has no USING/multi-table support (confirmed
        # empirically: NotImplementedError on a plain multi-table WHERE), so
        # this needs an EXISTS correlated subquery instead of Postgres's
        # DELETE ... USING.
        def _delete(extra: sa.ColumnElement[bool] | None = None) -> sa.Delete:
            conditions = (pk_match,) if extra is None else (pk_match, extra)
            return sa.delete(target).where(sa.exists().where(*conditions))

        if merge_batch_size is None:
            session.execute(_delete())
            return

        total = session.execute(sa.select(sa.func.count()).select_from(staging)).scalar_one()
        if total <= merge_batch_size:
            session.execute(_delete())
            return

        rowid = self._staging_rowid()
        start = 0
        while start < total:
            end = start + merge_batch_size
            session.execute(_delete(sa.and_(rowid > start, rowid <= end)))
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

        def _upsert(select_: sa.Select[Any]) -> sa.Insert:
            return (
                sqlite_dialect.insert(target)
                .from_select(insertable_cols, select_)
                .on_conflict_do_nothing(index_elements=pk_cols)
            )

        non_paginated_select = sa.select(*(staging.c[c] for c in insertable_cols))

        if merge_batch_size is None:
            # SQLite's grammar rejects INSERT...SELECT...ON CONFLICT with no
            # WHERE on the SELECT (confirmed empirically); sa.true() supplies one.
            session.execute(_upsert(non_paginated_select.where(sa.true())))
            return

        total = session.execute(sa.select(sa.func.count()).select_from(staging)).scalar_one()
        if total <= merge_batch_size:
            session.execute(_upsert(non_paginated_select.where(sa.true())))
            return

        rowid = self._staging_rowid()
        start = 0
        while start < total:
            end = start + merge_batch_size
            batch_select = non_paginated_select.where(rowid > start, rowid <= end)
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

        def _insert(select_: sa.Select[Any]) -> sa.Insert:
            return sa.insert(target).from_select(insertable_cols, select_)

        if merge_batch_size is None:
            session.execute(_insert(non_paginated_select))
            return

        total = session.execute(sa.select(sa.func.count()).select_from(staging)).scalar_one()
        if total <= merge_batch_size:
            session.execute(_insert(non_paginated_select))
            return

        rowid = self._staging_rowid()
        start = 0
        while start < total:
            end = start + merge_batch_size
            batch_select = non_paginated_select.where(rowid > start, rowid <= end)
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
        bind: "Engine | Connection",
        name: str,
        selectable: sa.sql.Select[Any],
        *,
        schema: str | None = None,
    ) -> None:
        self._require_capability("supports_materialized_views", "materialized views")

    def refresh_materialized_view(
        self,
        bind: "Engine | Connection",
        name: str,
        *,
        schema: str | None = None,
    ) -> None:
        self._require_capability("supports_materialized_views", "materialized views")

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
        raise_error: bool = True,
    ) -> None:
        bind: Engine | Connection = session.get_bind()
        if bind.dialect.name != "sqlite":
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
