"""Confirms orm-loader's STAGING_SCHEMA reservation (registered via
orm_loader.staging_schema_claim(), passed into a consumer's own
create_engine(schema_claims=[...]) call) is actually picked up by
oa-configurator's own conflict check: a CDM database resolving
cdm_schema=STAGING_SCHEMA on the same connection must raise, proving the
cross-package registration/enforcement wiring works end to end, not just
in isolation on either side.

Registration happens once, at create_engine() time, via the sanctioned
schema_claims= path -- create_staging_table() only reads the already-
registered schema (see backends/postgres.py), it never registers it
itself. Uses a fresh, genuinely committing engine (never the
rollback-protected pg_db.connection), since the registration write and
the later conflict check are two separate connections and must actually
see each other.
"""

from __future__ import annotations

from oa_configurator import (
    CDMDatabaseConfig,
    ConnectionConfig,
    GenericDatabaseConfig,
    Resolver,
    SchemaOwnershipError,
    StackConfig,
)
from oa_configurator.domains.resources.schema_registry import SchemaRegistry
from sqlalchemy.engine import make_url
from sqlalchemy import Table
from typing import cast
import pytest

from orm_loader.backends import STAGING_SCHEMA, staging_schema_claim

pytestmark = [pytest.mark.postgresql, pytest.mark.db_dialect]


def _connection_config(pg_db) -> ConnectionConfig:
    connection = pg_db.resolved.connection
    url = make_url(connection.url)
    return ConnectionConfig(
        dialect=url.drivername, host=url.host, port=url.port,
        user=url.username, password=url.password, database_name=url.database,
        test_only=connection.test_only,
    )


def test_resolving_cdm_database_with_staging_schema_name_raises(pg_db) -> None:
    connection_config = _connection_config(pg_db)
    stack = StackConfig.for_session(
        connections={"c": connection_config},
        databases={
            "loader": GenericDatabaseConfig(connection="c"),
            "default": CDMDatabaseConfig(connection="c", cdm_schema=STAGING_SCHEMA),
        },
    )
    resolver = Resolver(stack)
    engine = resolver.resolve_database("loader").create_engine(
        schema_claims=[staging_schema_claim()]
    )
    try:
        with pytest.raises(SchemaOwnershipError, match=f"{STAGING_SCHEMA!r}.*orm_loader"):
            resolver.resolve_database("default").create_engine()
    finally:
        engine.dispose()
        with pg_db.committing_engine.begin() as connection:
            table = cast(Table, SchemaRegistry.__table__)
            connection.execute(table.delete().where(table.c.physical_schema == STAGING_SCHEMA))
