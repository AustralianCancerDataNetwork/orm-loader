import logging
from collections.abc import Iterable
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
    referred_schema_tag,
    without_cross_engine_foreign_keys,
)

from .metadata import Base

logger = logging.getLogger(__name__)


def _resolve_binds(resolved: ResolvedDatabase, bindable: Bindable | None) -> tuple[Bindable, Bindable]:
    """Return (primary_bind, vocab_bind) to run DDL against.

    Built from resolved.create_engines()/create_engine() when bindable is
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


def create_tables(
    connection: sa.Connection,
    tables: Iterable[sa.Table],
    *,
    resolved: ResolvedDatabase,
) -> None:
    """Create *tables* on *connection* as they can physically exist under *resolved*.

    Foreign keys to a tag hosted on another database are left out, since no
    dialect can express them. When none cross, this creates the requested
    tables from each table's own metadata. Otherwise same-transaction tables
    from each requested table's metadata are copied alongside, so kept local
    foreign keys resolve even when their target tables aren't requested.

    Parameters
    ----------
    connection : sqlalchemy.Connection
        Open connection on the database hosting every table in *tables*.
    tables : Iterable[sqlalchemy.Table]
        ``Base`` tables to create, all hosted on *connection*'s database.
    resolved : ResolvedDatabase
        Topology answering which foreign keys can exist.

    Raises
    ------
    ValueError
        If a table has no schema tag.
    """
    tables = list(tables)
    tagged = [(table, table.schema) for table in tables]
    if not tagged:
        return
    untagged = sorted(table.name for table, tag in tagged if tag is None)
    if untagged:
        raise ValueError(f"create_tables() needs schema-tagged tables, got untagged {untagged}.")
    schema_tags = {tag for _, tag in tagged if tag is not None}
    if not resolved.tags_share_a_transaction(*schema_tags):
        raise ValueError(
            "create_tables() requires all tables to share one database transaction; "
            f"got schema tags {sorted(schema_tags)}."
        )
    crossing = any(
        not resolved.foreign_key_can_span(
            cast(str, table.schema), referred_schema_tag(fk, cast(str, table.schema))
        )
        for table in tables
        for fk in table.foreign_keys
    )
    if not crossing:
        tables_by_metadata: dict[sa.MetaData, list[sa.Table]] = {}
        for table in tables:
            tables_by_metadata.setdefault(table.metadata, []).append(table)
        for metadata, metadata_tables in tables_by_metadata.items():
            metadata.create_all(connection, tables=metadata_tables, checkfirst=True)
        return

    related_by_metadata: dict[sa.MetaData, set[sa.Table]] = {}
    requested_by_metadata: dict[sa.MetaData, set[str]] = {}
    for table in tables:
        metadata = table.metadata
        requested_by_metadata.setdefault(metadata, set()).add(table.key)
        related_by_metadata.setdefault(metadata, set()).update(
            candidate
            for candidate in metadata.tables.values()
            if candidate.schema is not None
            and resolved.tags_share_a_transaction(
                candidate.schema, cast(str, table.schema)
            )
        )

    for metadata, related in related_by_metadata.items():
        local_tables = [table for table in metadata.tables.values() if table in related]
        copies = without_cross_engine_foreign_keys(local_tables, resolved=resolved)
        requested = [table for table in copies if table.key in requested_by_metadata[metadata]]
        copies[0].metadata.create_all(connection, tables=requested, checkfirst=True)


def create_db(resolved: ResolvedDatabase, *, bindable: Bindable | None = None) -> None:
    """Create every schema-tagged table in Base.metadata.

    Schema creation (CREATE SCHEMA) happens inside engine creation when
    this function builds its own engine(s). This function's only job is
    metadata.create_all() for the tables themselves, grouped by schema
    tag, each group created through :func:`create_tables`.

    Notes
    -----
    Because the engine(s) used here are always freshly built for this
    one call (or explicitly handed in), there's no window for schema drift
    between the engine's creation and this call's create_all(), so no
    separate provenance guard is needed here.
    """
    if not isinstance(resolved, ResolvedDatabase):
        raise TypeError(
            "create_db() requires a ResolvedDatabase; pass bindable= for an existing engine."
        )
    if (
        bindable is not None
        and isinstance(resolved, ResolvedCDMDatabase)
        and not resolved.tags_share_a_transaction("primary", "vocab")
    ):
        raise ValueError(
            "create_db() cannot use one bindable for a split database: one bind cannot host "
            "both databases. Omit bindable so each role gets its own engine."
        )
    logger.debug("Creating database schema")
    if not Base.metadata.tables:
        logger.warning(
            "create_db(): Base.metadata declares no tables. If this is unexpected, "
            "the module(s) defining your ORM models likely weren't imported yet. "
            "SQLAlchemy only registers a model on Base when its module runs."
        )
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
                        "create_engines(schema_claims=[...]) call."
                    )
                create_tables(connection, tables, resolved=resolved)
    finally:
        if owned:
            # _resolve_binds() only returns Engine instances on this path
            # (bindable is None): create_engines()/create_engine().
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
    if not isinstance(resolved, ResolvedDatabase):
        raise TypeError(
            "bootstrap() requires a ResolvedDatabase; pass bindable= for an existing engine."
        )
    if create:
        create_db(resolved, bindable=bindable)
