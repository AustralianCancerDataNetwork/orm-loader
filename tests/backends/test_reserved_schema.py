"""Confirms orm-loader's STAGING_SCHEMA registration (backends/base.py,
Phase 2.3) is actually picked up by oa-configurator's reserved-schema
check: resolving a CDM database configured with schema_name="staging"
must raise, proving the cross-package registration/enforcement wiring
works end to end, not just in isolation on either side.
"""

from __future__ import annotations

import pytest
from oa_configurator import CDMDatabaseConfig, ConnectionConfig, StackConfig
from pydantic import ValidationError

from orm_loader.backends import STAGING_SCHEMA


def test_resolving_cdm_database_with_staging_schema_name_raises() -> None:
    with pytest.raises(ValidationError, match=f"{STAGING_SCHEMA!r}.*orm-loader"):
        StackConfig.for_session(
            connections={"c": ConnectionConfig(dialect="sqlite", database_name=":memory:")},
            databases={"default": CDMDatabaseConfig(connection="c", schema_name=STAGING_SCHEMA)},
        )
