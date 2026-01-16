from contextlib import contextmanager
from sqlalchemy import text, Engine
from sqlalchemy.orm import Session
import sqlalchemy as sa
from .logging import get_logger

logger = get_logger(__name__)

@contextmanager
def bulk_load_context(
    session: Session,
    *,
    disable_fk: bool = True,
    no_autoflush: bool = True,
):
    engine = session.get_bind()
    dialect = engine.dialect.name
    fk_disabled = False

    try:
        if disable_fk:
            if dialect == "postgresql":
                session.execute(text(
                    "SET session_replication_role = replica"
                ))
                fk_disabled = True
            elif dialect == "sqlite":
                session.execute(text("PRAGMA foreign_keys = OFF"))
                fk_disabled = True
            
            logger.info("Disabled foreign key checks for bulk load")

        if no_autoflush:
            with session.no_autoflush:
                yield
        else:
            yield

    except Exception:
        session.rollback()
        raise

    finally:
        if fk_disabled:
            if dialect == "postgresql":
                session.execute(text(
                    "SET session_replication_role = DEFAULT"
                ))
            elif dialect == "sqlite":
                session.execute(text("PRAGMA foreign_keys = ON"))

            logger.info("Re-enabled foreign key checks after bulk load")

@contextmanager
def engine_with_replica_role(engine: Engine):
    """
    Context manager that:
    - forces session_replication_role=replica on all connections
    - restores DEFAULT on exit
    
    this is different to bulk_load_context manager from orm_loader.helpers 
    because this is engine scoped where that one is session scoped

    postgres only
    """

    @sa.event.listens_for(engine, "connect") # type: ignore
    def _set_replica_role(dbapi_conn, _):
        cur = dbapi_conn.cursor()
        cur.execute("SET session_replication_role = replica")
        cur.close()

    try:
        yield engine
    finally:
        # Explicitly restore on a fresh connection
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(text("SET session_replication_role = DEFAULT"))

            role = conn.execute(
                text("SHOW session_replication_role")
            ).scalar()

            if role != "origin":
                raise RuntimeError(
                    "Failed to restore session_replication_role"
                )

        logger.info("session_replication_role restored to DEFAULT")
