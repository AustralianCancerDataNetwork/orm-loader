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
    GenericDatabaseConfig,
    Resolver,
    SchemaOwnershipError,
)
from oa_configurator.testing import reset_schema_registry_rows
import pytest

from orm_loader.backends import STAGING_SCHEMA, staging_schema_claim

pytestmark = [pytest.mark.postgresql, pytest.mark.db_dialect]


def test_resolving_cdm_database_with_staging_schema_name_raises(pg_db, cleanup_after_test) -> None:
    """
    Notes
    -----
    Overrides pg_db's own CDM entry in place, since two
    CDM entries sharing one physical connection is itself unconfigurable."""
    reset_schema_registry_rows(cleanup_after_test, pg_db.committing_engine, [STAGING_SCHEMA])
    connection = pg_db.resolved.connection.name
    cdm_name = pg_db.resolved.name
    resolver = Resolver.from_active_config().with_overrides(
        databases={
            "loader": GenericDatabaseConfig(connection=connection),
            cdm_name: CDMDatabaseConfig(connection=connection, cdm_schema=STAGING_SCHEMA),
        },
    )
    engine = resolver.resolve_database("loader").create_engine(
        schema_claims=[staging_schema_claim()]
    )
    try:
        with pytest.raises(SchemaOwnershipError, match=f"{STAGING_SCHEMA!r}.*orm_loader"):
            resolver.resolve_database(cdm_name).create_engines()
    finally:
        engine.dispose()
