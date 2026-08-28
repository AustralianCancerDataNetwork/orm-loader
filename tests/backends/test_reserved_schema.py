"""Confirms orm-loader's STAGING_SCHEMA registration (backends/base.py,
Phase 2.3) is actually picked up by oa-configurator's reserved-schema
check: resolving a CDM database configured with schema_name="staging"
must raise, proving the cross-package registration/enforcement wiring
works end to end, not just in isolation on either side.
"""

from __future__ import annotations

import pytest
from oa_configurator import CDMDatabaseConfig, ConnectionConfig, Resolver, StackConfig

from orm_loader.backends import STAGING_SCHEMA


def test_resolving_cdm_database_with_staging_schema_name_raises() -> None:
    cfg = StackConfig.for_session(
        connections={"c": ConnectionConfig(dialect="sqlite", database_name=":memory:")},
        databases={"default": CDMDatabaseConfig(connection="c", schema_name=STAGING_SCHEMA)},
    )
    with pytest.raises(RuntimeError, match=f"{STAGING_SCHEMA!r}.*orm-loader"):
        Resolver(cfg).resolve_database("default")
