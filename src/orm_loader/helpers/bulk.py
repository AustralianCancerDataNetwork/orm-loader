from contextlib import contextmanager
from sqlalchemy import text, Engine
from sqlalchemy.orm import Session
import sqlalchemy as sa
from .logging import get_logger

logger = get_logger(__name__)

def disable_fk_check(session: Session) -> str | int:
    """Disables FK checks and returns the previous state."""
    engine = session.get_bind()
    dialect = engine.dialect.name
    previous_state = None

    if dialect == "postgresql":
        previous_state = session.execute(text("SHOW session_replication_role")).scalar()
        session.execute(text("SET session_replication_role = 'replica'"))
    elif dialect == "sqlite":
        previous_state = session.execute(text("PRAGMA foreign_keys")).scalar()
        session.execute(text("PRAGMA foreign_keys = OFF"))
    else:
        raise NotImplementedError(f"FK disable not implemented for {dialect}")
        
    logger.info("Disabled foreign key checks for bulk load.")
    assert isinstance(previous_state, (str, int)), "Expected previous FK state to be str or int"
    return previous_state

def enable_fk_check(session: Session) -> str | int:
    """Explicitly enables FK checks and returns the previous state."""
    engine = session.get_bind()
    dialect = engine.dialect.name
    previous_state = None

    if dialect == "postgresql":
        previous_state = session.execute(text("SHOW session_replication_role")).scalar()
        session.execute(text("SET session_replication_role = 'origin'"))
    elif dialect == "sqlite":
        previous_state = session.execute(text("PRAGMA foreign_keys")).scalar()
        session.execute(text("PRAGMA foreign_keys = ON"))
    else:
        raise NotImplementedError(f"FK enable not implemented for {dialect}")
        
    logger.info("Explicitly re-enabled foreign key checks.")
    assert isinstance(previous_state, (str, int)), "Expected previous FK state to be str or int"
    return previous_state

def restore_fk_check(session: Session, previous_state: str | int):
    """Restores FK checks to a specifically provided previous state."""
    engine = session.get_bind()
    dialect = engine.dialect.name

    if dialect == "postgresql":
        session.execute(text(f"SET session_replication_role = '{previous_state}'"))
    elif dialect == "sqlite":
        session.execute(text(f"PRAGMA foreign_keys = {previous_state}"))
    else:
        raise NotImplementedError(f"FK restore not implemented for {dialect}")
        
    logger.info(f"Restored foreign key checks to state: {previous_state}")

@contextmanager
def bulk_load_context(
    session: Session,
    *,
    disable_fk: bool = True,
    no_autoflush: bool = True,
):
    previous_fk_state = None
    try:
        if disable_fk:
            previous_fk_state = disable_fk_check(session)

        if no_autoflush:
            with session.no_autoflush:
                yield
        else:
            yield

    except Exception:
        session.rollback()
        raise

    finally:
        if previous_fk_state is not None:
            restore_fk_check(session, previous_fk_state)


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
