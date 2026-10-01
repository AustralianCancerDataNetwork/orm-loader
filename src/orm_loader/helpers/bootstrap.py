from contextlib import ExitStack
import logging

import sqlalchemy as sa
from oa_configurator import (
    ResolvedCDMDatabase,
    ResolvedDatabase,
    guard_schema_provenance_for,
    is_ephemeral_url,
    open_connection,
)

from .metadata import Base

logger = logging.getLogger(__name__)

Bindable = sa.engine.Engine | sa.engine.Connection


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
    logger.debug("Creating database schema")
    primary_bind, vocab_bind = _resolve_binds(resolved, bindable)

    tables_by_bind: dict[int, tuple[Bindable, dict[str, list[sa.Table]]]] = {}
    for table in Base.metadata.tables.values():
        schema_tag = table.schema
        if schema_tag is None:
            continue
        bind = resolved.route_for_schema_tag(schema_tag, vocab=vocab_bind, primary=primary_bind)
        _, tables_by_tag = tables_by_bind.setdefault(id(bind), (bind, {}))
        tables_by_tag.setdefault(schema_tag, []).append(table)

    with ExitStack() as guard_stack:
        for bind, tables_by_tag in tables_by_bind.values():
            connection = guard_stack.enter_context(open_connection(bind))
            # One provenance guard per schema_tag on this connection; ExitStack defers every write until the block below succeeds.
            for schema_tag in tables_by_tag:
                guard_stack.enter_context(
                    guard_schema_provenance_for(connection, resolved, schema_tag=schema_tag)
                )
            tables = [table for tables in tables_by_tag.values() for table in tables]
            Base.metadata.create_all(connection, tables=tables, checkfirst=True)


def bootstrap(
    resolved: ResolvedDatabase,
    *,
    create: bool = True,
    bindable: Bindable | None = None,
) -> None:
    logger.info("Bootstrapping schema (create=%s)", create)
    if create:
        create_db(resolved, bindable=bindable)
