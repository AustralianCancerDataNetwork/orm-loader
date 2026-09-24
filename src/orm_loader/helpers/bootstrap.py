from contextlib import ExitStack
import logging

import sqlalchemy as sa
from oa_configurator import ResolvedDatabase, guard_schema_provenance_for, open_connection, validate_schema_tag

from .metadata import Base

logger = logging.getLogger(__name__)


def create_db(
    bindable: sa.engine.Engine | sa.engine.Connection, *, resolved: ResolvedDatabase | None = None
) -> None:
    logger.debug("Creating database schema")
    tables_by_schema_tag: dict[str, list[sa.Table]] = {}
    for table in Base.metadata.tables.values():
        schema_tag = validate_schema_tag(table)
        if schema_tag is not None:
            tables_by_schema_tag.setdefault(schema_tag, []).append(table)

    with open_connection(bindable) as connection, ExitStack() as guard_stack:
        # One provenance guard per schema_tag (count only known at runtime); ExitStack defers every write until the block below succeeds.
        for schema_tag, tables in tables_by_schema_tag.items():
            # A same-named table could already exist under a drifted schema, attached to an unrelated table.
            guard_stack.enter_context(
                guard_schema_provenance_for(connection, resolved, schema_tag=schema_tag, tables=tables)
            )
        Base.metadata.create_all(connection)


def bootstrap(
    bindable: sa.engine.Engine | sa.engine.Connection,
    *,
    create: bool = True,
    resolved: ResolvedDatabase | None = None,
) -> None:
    logger.info("Bootstrapping schema (create=%s)", create)
    if create:
        create_db(bindable, resolved=resolved)
