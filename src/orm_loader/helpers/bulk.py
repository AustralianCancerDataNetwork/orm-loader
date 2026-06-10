import logging
from contextlib import contextmanager
from sqlalchemy import Engine
from sqlalchemy.orm import Session
from typing import Iterator
from ..backends.resolve import resolve_backend

logger = logging.getLogger(__name__)

def disable_fk_check(session: Session) -> str | int:
    """Disable foreign-key checks for the current session and return the previous state."""
    previous_state = resolve_backend(session).disable_fk_check(session)
    logger.info("Disabled foreign key checks for bulk load.")
    if not isinstance(previous_state, (str, int)):
        logger.error(f"Unexpected FK state type: {type(previous_state)}. Expected str or int.")
        raise TypeError(f"Expected previous FK state to be str or int, got {type(previous_state)}")
    return previous_state

def enable_fk_check(session: Session) -> str | int:
    """Enable foreign-key checks for the current session and return the previous state."""
    previous_state = resolve_backend(session).enable_fk_check(session)
    logger.info("Explicitly re-enabled foreign key checks.")
    if not isinstance(previous_state, (str, int)):
        logger.error(f"Unexpected FK state type: {type(previous_state)}. Expected str or int.")
        raise TypeError(f"Expected previous FK state to be str or int, got {type(previous_state)}")
    return previous_state

def restore_fk_check(session: Session, previous_state: str | int):
    """Restore foreign-key checks to a previously captured backend-specific state."""
    resolve_backend(session).restore_fk_check(session, previous_state)
    logger.info(f"Restored foreign key checks to state: {previous_state}")

@contextmanager
def bulk_load_context(
    session: Session,
    *,
    disable_fk: bool = True,
    no_autoflush: bool = True,
) -> Iterator[None]:
    """
    Wrap a trusted bulk operation in backend-aware session settings.

    This is a thin helper over ``DatabaseBackend.bulk_load_context()``.
    It exists so older call sites can keep using the helper import path.
    """
    backend = resolve_backend(session)
    with backend.bulk_load_context(
        session,
        disable_fk=disable_fk,
        no_autoflush=no_autoflush,
    ):
        yield


@contextmanager
def engine_with_replica_role(engine: Engine) -> Iterator[Engine]:
    """
    Force ``session_replication_role=replica`` on PostgreSQL engine connections.

    This is engine-scoped rather than session-scoped. It is only available
    on backends that explicitly implement the behaviour.
    """

    backend = resolve_backend(engine)
    method = getattr(backend, "engine_with_replica_role", None)
    if method is None:
        raise NotImplementedError(
            f"Backend '{backend.name}' does not support replica-role engine contexts"
        )
    with method(engine) as wrapped:
        yield wrapped
