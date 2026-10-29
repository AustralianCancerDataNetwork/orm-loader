from contextlib import contextmanager
from sqlalchemy import Engine
from sqlalchemy.orm import Session
from typing import Generator


@contextmanager
def bulk_load_context(
    session: Session,
    *,
    disable_fk: bool = True,
    no_autoflush: bool = True,
) -> Generator[None, None, None]:
    """
    Wrap a trusted bulk operation in backend-aware session settings.

    Foreign-key checks are disabled and autoflush is suspended for the
    duration, then restored. See ``DatabaseBackend.bulk_load_context``.
    """
    from ..backends.resolve import resolve_backend
    backend = resolve_backend(session)
    with backend.bulk_load_context(
        session,
        disable_fk=disable_fk,
        no_autoflush=no_autoflush,
    ):
        yield


@contextmanager
def engine_with_replica_role(engine: Engine) -> Generator[Engine, None, None]:
    """
    Force ``session_replication_role=replica`` on PostgreSQL engine connections.

    This is engine-scoped rather than session-scoped. It is only available
    on backends that explicitly implement the behaviour.
    """
    from ..backends.resolve import resolve_backend
    backend = resolve_backend(engine)
    method = getattr(backend, "engine_with_replica_role", None)
    if method is None:
        raise NotImplementedError(
            f"Backend '{backend.name}' does not support replica-role engine contexts"
        )
    with method(engine) as wrapped:
        yield wrapped
