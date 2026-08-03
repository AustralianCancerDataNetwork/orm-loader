"""Configuration for orm-loader via oa-configurator."""

from __future__ import annotations

from typing import ClassVar

from oa_configurator import PackageConfigBase, with_test_prefix


class OrmLoaderConfig(PackageConfigBase):
    """oa-configurator config class for orm-loader.

    orm-loader is connection-agnostic — it accepts SQLAlchemy sessions/engines
    as parameters and owns no database resources. This class exists to register
    orm-loader in the oa-configurator ecosystem, provide a canonical
    ``configure_logging()`` entry point, and name the test database used by the
    integration test suite.
    """

    tool_name: ClassVar[str] = "orm_loader"
    extra_logging_namespaces: ClassVar[tuple[str, ...]] = ()
    TEST_DB: ClassVar[str] = with_test_prefix("orm_db")
