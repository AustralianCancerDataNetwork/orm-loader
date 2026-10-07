import logging
from typing import cast

import sqlalchemy as sa
from oa_configurator import (
    Bindable,
    ResolvedCDMDatabase,
    ResolvedDatabase,
    UnregisteredSchemaTagError,
    claimed_schema_tags,
    declared_schema_tags,
    is_ephemeral_url,
    open_connection,
)

from .metadata import Base

logger = logging.getLogger(__name__)


def _resolve_binds(resolved: ResolvedDatabase, bindable: Bindable | None) -> tuple[Bindable, Bindable]:
    """Return (primary_bind, vocab_bind) to run DDL against.

    Built from resolved.create_engine()/create_engines() when bindable is
    omitted, keeping schema-claim registration and vocab/primary routing in
    one place. This is the default, enforced path for a real database!

    bindable is required instead whenever resolved can't be safely
    reconnected to: an ephemeral SQLite connection (':memory:', including
    shared-cache mode) gets a separate, empty database from a second,
    independently-built engine, since nothing else shares its data. bindable
    is also accepted for a non-ephemeral resolved when the caller already
    holds the one true engine and resolved can't be trusted to describe it
    faithfully.
    """
    if is_ephemeral_url(resolved.connection.safe_url) and bindable is None:
        raise ValueError(
            f"{resolved.name!r} resolves to an ephemeral connection (SQLite ':memory:' "
            "or shared-cache mode): a freshly built engine would be a separate, empty "
            "database. Pass the caller's own already-open engine/connection via `bindable`."
        )
    if bindable is not None:
        return bindable, bindable
    if isinstance(resolved, ResolvedCDMDatabase):
        return resolved.create_engines()
    engine = resolved.create_engine()
    return engine, engine


def create_db(resolved: ResolvedDatabase, *, bindable: Bindable | None = None) -> None:
    """Create every schema-tagged table in Base.metadata.

    Schema creation (CREATE SCHEMA) happens inside create_engine() when
    this function builds its own engine(s). This function's only job is
    metadata.create_all() for the tables themselves, grouped by schema
    tag. Because the engine(s) used here are always freshly built for this
    one call (or explicitly handed in), there's no window for schema drift
    between the engine's creation and this call's create_all(), so no
    separate provenance guard is needed here, unlike a long-lived engine
    reused across many later calls.
    """
    logger.debug("Creating database schema")
    owned = bindable is None
    primary_bind, vocab_bind = _resolve_binds(resolved, bindable)

    tables_by_bind: dict[int, tuple[Bindable, dict[str, list[sa.Table]]]] = {}
    untagged: list[str] = []
    for table in Base.metadata.tables.values():
        schema_tag = table.schema
        if schema_tag is None:
            untagged.append(table.name)
            continue
        bind = resolved.route_for_schema_tag(schema_tag, vocab=vocab_bind, primary=primary_bind)
        _, tables_by_tag = tables_by_bind.setdefault(id(bind), (bind, {}))
        tables_by_tag.setdefault(schema_tag, []).append(table)

    if untagged:
        raise ValueError(
            f"create_db() found table(s) with no schema tag (Table.schema is None): "
            f"{sorted(untagged)}. Every table must declare its schema_tag; an untagged "
            "table would otherwise be silently excluded from create_db()."
        )

    try:
        for bind, tables_by_tag in tables_by_bind.values():
            with open_connection(bind) as connection:
                tables = [table for tables in tables_by_tag.values() for table in tables]
                missing = declared_schema_tags(tables) - claimed_schema_tags(connection)
                if missing:
                    raise UnregisteredSchemaTagError(
                        f"create_db(): table(s) declare schema tag(s) {sorted(missing)} that "
                        "aren't claimed on this connection. Add them to your own "
                        "create_engine(schema_claims=[...]) call."
                    )
                Base.metadata.create_all(connection, tables=tables, checkfirst=True)
    finally:
        if owned:
            # _resolve_binds() only returns Engine instances on this path
            # (bindable is None): create_engine()/create_engines().
            cast(sa.engine.Engine, primary_bind).dispose()
            if vocab_bind is not primary_bind:
                cast(sa.engine.Engine, vocab_bind).dispose()


def bootstrap(
    resolved: ResolvedDatabase,
    *,
    create: bool = True,
    bindable: Bindable | None = None,
) -> None:
    logger.info("Bootstrapping schema (create=%s)", create)
    if create:
        create_db(resolved, bindable=bindable)
