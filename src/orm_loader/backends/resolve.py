from __future__ import annotations

from typing import TYPE_CHECKING
import sqlalchemy.orm as so

from .base import DatabaseBackend, Dialect
from .postgres import PostgresBackend
from .sqlite import SQLiteBackend

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine


_BACKEND_TYPES: dict[Dialect, type[DatabaseBackend]] = {
    Dialect.POSTGRESQL: PostgresBackend,
    Dialect.SQLITE: SQLiteBackend,
}


def _dialect(bindable: "so.Session | Engine | Connection") -> Dialect:
    if isinstance(bindable, so.Session):
        bind = bindable.get_bind()
        dialect_name = bind.dialect.name
    elif hasattr(bindable, "dialect"):
        dialect_name = bindable.dialect.name
    else:
        raise TypeError(f"Unsupported bindable type: {type(bindable)!r}")

    try:
        return Dialect(dialect_name)
    except ValueError as exc:
        raise NotImplementedError(
            f"Unsupported SQLAlchemy dialect '{dialect_name}'"
        ) from exc


def resolve_backend(bindable: "so.Session | Engine | Connection", **kwargs) -> DatabaseBackend:
    """Resolve a concrete backend from a SQLAlchemy session, engine, or connection."""
    dialect = _dialect(bindable)
    try:
        return _BACKEND_TYPES[dialect](**kwargs)
    except KeyError:
        raise NotImplementedError(f"No backend registered for dialect '{dialect.value}'")
