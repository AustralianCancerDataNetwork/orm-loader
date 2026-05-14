from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy import event, text
from sqlalchemy.exc import IntegrityError

from .base import BackendCapabilities, DatabaseBackend

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine

    from ..tables.typing import CSVTableProtocol


logger = logging.getLogger(__name__)
VALID_SQLITE_JOURNAL_MODES = frozenset(
    {"DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF"}
)


class SQLiteBackend(DatabaseBackend):
    def __init__(
        self,
        *,
        busy_timeout_ms: int = 60000,
        journal_mode: str = "WAL",
        defer_foreign_keys: bool = True,
    ) -> None:
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

    @property
    def name(self) -> str:
        return "sqlite"

    @property
    def dialect_names(self) -> tuple[str, ...]:
        return ("sqlite",)

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
        staging_name: str,
    ) -> None:
        session.execute(sa.text(f'DROP TABLE IF EXISTS "{staging_name}";'))

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
        session: so.Session,
        staging_name: str,
    ) -> None:
        session.execute(sa.text(f'DROP TABLE IF EXISTS "{staging_name}"'))

    def disable_fk_check(self, session: so.Session) -> str | int:
        previous_state = session.execute(text("PRAGMA foreign_keys")).scalar()
        session.execute(text("PRAGMA foreign_keys = OFF"))
        assert isinstance(previous_state, int), "Expected SQLite FK state to be an int"
        return previous_state

    def enable_fk_check(self, session: so.Session) -> str | int:
        previous_state = session.execute(text("PRAGMA foreign_keys")).scalar()
        session.execute(text("PRAGMA foreign_keys = ON"))
        assert isinstance(previous_state, int), "Expected SQLite FK state to be an int"
        return previous_state

    def restore_fk_check(
        self,
        session: so.Session,
        previous_state: str | int,
    ) -> None:
        session.execute(text(f"PRAGMA foreign_keys = {previous_state}"))

    def merge_replace(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
        target_name: str,
        staging_name: str,
        pk_cols: list[str],
    ) -> None:
        if len(pk_cols) == 1:
            pk = pk_cols[0]
            session.execute(
                sa.text(
                    f"""
                    DELETE FROM "{target_name}"
                    WHERE "{pk}" IN (
                        SELECT "{pk}" FROM "{staging_name}"
                    );
                    """
                )
            )
            return

        pk_match = " AND ".join(
            f'"{target_name}"."{c}" = "{staging_name}"."{c}"' for c in pk_cols
        )
        session.execute(
            sa.text(
                f"""
                DELETE FROM "{target_name}"
                WHERE EXISTS (
                    SELECT 1 FROM "{staging_name}"
                    WHERE {pk_match}
                );
                """
            )
        )

    def merge_upsert(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
        target_name: str,
        staging_name: str,
        pk_cols: list[str],
    ) -> None:
        insertable_cols = self._insertable_column_names(table_cls)
        cols_str = ", ".join(f'"{c}"' for c in insertable_cols)
        session.execute(
            sa.text(
                f"""
                INSERT OR IGNORE INTO "{target_name}" ({cols_str})
                SELECT {cols_str} FROM "{staging_name}";
                """
            )
        )

    def merge_insert(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
        target_name: str,
        staging_name: str,
    ) -> None:
        insertable_cols = self._insertable_column_names(table_cls)
        cols_str = ", ".join(f'"{c}"' for c in insertable_cols)
        session.execute(
            sa.text(
                f"""
                INSERT INTO "{target_name}" ({cols_str})
                SELECT {cols_str} FROM "{staging_name}";
                """
            )
        )

    def merge_context(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
    ):
        return self.bulk_load_context(session, disable_fk=True, no_autoflush=False)

    def create_materialized_view(
        self,
        bind: "Engine | Connection",
        name: str,
        selectable: sa.sql.Select[Any],
    ) -> None:
        self._require_capability("supports_materialized_views", "materialized views")

    def refresh_materialized_view(
        self,
        bind: "Engine | Connection",
        name: str,
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
        def _enable_sqlite_foreign_keys( # type: ignore
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
