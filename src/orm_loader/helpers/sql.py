from __future__ import annotations

from sqlalchemy.sql.compiler import IdentifierPreparer


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
