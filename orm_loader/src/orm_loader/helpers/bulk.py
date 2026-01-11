from contextlib import contextmanager
from sqlalchemy import text
from sqlalchemy.orm import Session

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
