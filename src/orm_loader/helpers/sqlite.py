from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from ..backends.sqlite import SQLiteBackend

def enable_sqlite_foreign_keys(
    dbapi_connection: Any,
    connection_record: Any,
) -> None:
    """
    Apply the default SQLite connection settings used by orm-loader.

    This helper is kept for compatibility with older event-hook setups.
    It delegates to ``SQLiteBackend.configure_dbapi_connection()``,
    which may apply more than just foreign-key settings.
    """
    del connection_record
    SQLiteBackend().configure_dbapi_connection(dbapi_connection)


def attach_sqlite_bulk_load_pragmas(
    engine: Engine,
    *,
    busy_timeout_ms: int = 60000,
    journal_mode: str = "WAL",
    defer_foreign_keys: bool = True,
) -> None:
    """
    Install SQLite connect hooks aimed at heavy local write workloads.

    The hook currently sets ``busy_timeout`` and journal mode, and can
    also enable deferred foreign-key checking for the connection.
    """
    SQLiteBackend(
        busy_timeout_ms=busy_timeout_ms,
        journal_mode=journal_mode,
        defer_foreign_keys=defer_foreign_keys,
    ).install_engine_hooks(engine)


def explain_sqlite_fk_error(session, exc: IntegrityError, raise_error: bool = True):
    """Log SQLite foreign-key check details before re-raising an error."""
    SQLiteBackend().explain_fk_error(session, exc, raise_error=raise_error)


def restore_sqlite_journal_mode(db_path: Path) -> None:
    """
    Checkpoint WAL contents and switch the database back to ``DELETE`` mode.

    Call this after disposing active SQLite connections. Reconnecting
    through an engine that still installs WAL hooks will enable WAL again.
    """
    SQLiteBackend().restore_journal_mode(db_path)
