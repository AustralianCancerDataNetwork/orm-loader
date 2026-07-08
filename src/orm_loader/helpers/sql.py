from __future__ import annotations


def qualify_identifier(name: str, schema: str | None) -> str:
    """
    Return a double-quoted, optionally schema-qualified SQL identifier.

    Parameters
    ----------
    name
        The SQL identifier to qualify (e.g. a table name).
    schema
        Schema name to prefix. If None, returns only the quoted identifier.
        Useful for backends that do not support schema-qualified identifiers (e.g. SQLite).

    Returns
    -------
    str
        e.g. '"staging"."_staging_foo"' or '"_staging_foo"'.
    """
    return f'"{schema}"."{name}"' if schema else f'"{name}"'
