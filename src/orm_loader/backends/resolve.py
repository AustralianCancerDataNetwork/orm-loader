from __future__ import annotations

from typing import TYPE_CHECKING
import sqlalchemy.orm as so

from .base import DatabaseBackend
from .postgres import PostgresBackend
from .sqlite import SQLiteBackend

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine


_BACKEND_TYPES: tuple[type[DatabaseBackend], ...] = (
    PostgresBackend,
    SQLiteBackend,
)


def _dialect_name(bindable: so.Session | "Engine" | "Connection") -> str:
    if isinstance(bindable, so.Session):
        bind = bindable.get_bind()
        return bind.dialect.name

    if hasattr(bindable, "dialect"):
        return bindable.dialect.name

    raise TypeError(f"Unsupported bindable type: {type(bindable)!r}")


def resolve_backend(bindable: so.Session | "Engine" | "Connection") -> DatabaseBackend:
    """
    Resolve a concrete backend from a SQLAlchemy session, engine, or connection.
    """
    dialect_name = _dialect_name(bindable)
    for backend_type in _BACKEND_TYPES:
        backend = backend_type()
        if backend.supports_dialect(dialect_name):
            return backend
    raise NotImplementedError(f"No backend registered for dialect '{dialect_name}'")
