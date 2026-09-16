from __future__ import annotations

import sqlalchemy as sa
from oa_configurator import Role
from sqlalchemy.sql.compiler import IdentifierPreparer


def role_of_table(table: sa.Table) -> Role:
    """The ``Role`` a mapped table's own declared schema tag names.

    Every real CDM table is tagged ``schema=Role.X.value`` at class
    definition time (Phase 2's schema-role parity work); reading it back off
    the ``Table`` itself is the source of truth for which schema_translate_map
    key a raw-SQL/reflection call site should resolve through, rather than
    always defaulting to primary or reintroducing a manually-threaded
    parameter that could disagree with what the table actually declares.
    Falls back to ``Role.PRIMARY`` for a table with no schema tag at all
    (schema=None), matching schema_of()'s own default.
    """
    if table.schema is None:
        return Role.PRIMARY
    return Role(table.schema)


def qualify_identifier(name: str, schema: str | None, preparer: IdentifierPreparer) -> str:
    """
    Return a quoted, optionally schema-qualified SQL identifier.

    Parameters
    ----------
    name
        The SQL identifier to qualify (e.g. a table name).
    schema
        Schema name to prefix. If None, returns only the quoted identifier.
        Useful for backends that do not support schema-qualified identifiers (e.g. SQLite).
    preparer
        The dialect-specific identifier preparer used to quote and escape each
        component. Delegating to SQLAlchemy here (rather than hand-rolled
        f-string quoting) ensures embedded quote characters are escaped
        correctly for the target dialect.

    Returns
    -------
    str
        e.g. '"staging"."_staging_foo"' or '"_staging_foo"'.
    """
    if schema:
        return f"{preparer.quote_identifier(schema)}.{preparer.quote_identifier(name)}"
    return preparer.quote_identifier(name)
