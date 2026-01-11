from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
import logging

logger = logging.getLogger(__name__)

@event.listens_for(Engine, "connect")
def enable_sqlite_foreign_keys(dbapi_connection, connection_record):
    if dbapi_connection.__class__.__module__.startswith("sqlite3"):
        logger.debug("Enabling SQLite foreign key enforcement")
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA defer_foreign_keys = ON;")
        cursor.close()

def explain_sqlite_fk_error(session, exc: IntegrityError, raise_error: bool = True):
    engine = session.get_bind()
    if engine.dialect.name != "sqlite":
        raise exc

    with engine.connect() as conn:
        rows = conn.execute(text("PRAGMA foreign_key_check")).fetchall()

    if rows:
        for r in rows:
            logger.error(
                "FK violation: table=%s rowid=%s references=%s fk_index=%s",
                r[0], r[1], r[2], r[3]
            )

    if raise_error:
        raise exc
