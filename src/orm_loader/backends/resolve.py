from __future__ import annotations

import sqlalchemy.orm as so

from oa_configurator import Bindable, UnregisteredSchemaTagError, physical_schema_of

from .base import DatabaseBackend, Dialect
from .postgres import PostgresBackend
from .sqlite import SQLiteBackend


_BACKEND_TYPES: dict[Dialect, type[DatabaseBackend]] = {
    Dialect.POSTGRESQL: PostgresBackend,
    Dialect.SQLITE: SQLiteBackend,
}


def _dialect(bindable: Bindable) -> Dialect:
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


def resolve_backend(
    bindable: Bindable,
    *,
    staging_schema_tag: str | None = None,
    **kwargs,
) -> DatabaseBackend:
    """Resolve a concrete backend from a SQLAlchemy session, engine, or connection.

    staging_schema_tag is resolved to its physical schema here, as it is the common
    entry point for all backends.

    Raises
    ------
    oa_configurator.UnregisteredSchemaTagError
        If staging_schema_tag was never reserved on bindable via
        ``create_engines(schema_claims=[staging_schema_claim()])``.
    """
    dialect = _dialect(bindable)
    staging_schema: str | None = None
    if staging_schema_tag is not None:
        try:
            staging_schema = physical_schema_of(bindable, schema_tag=staging_schema_tag)
        except UnregisteredSchemaTagError as exc:
            raise UnregisteredSchemaTagError(
                "orm-loader needs its staging schema reserved on this engine. Add "
                "orm_loader.staging_schema_claim() to your own "
                "create_engines(schema_claims=[...]) call."
            ) from exc
    try:
        return _BACKEND_TYPES[dialect](
            staging_schema_tag=staging_schema_tag, staging_schema=staging_schema, **kwargs
        )
    except KeyError:
        raise NotImplementedError(f"No backend registered for dialect '{dialect.value}'")
