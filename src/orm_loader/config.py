"""Configuration for orm-loader via oa-configurator."""

from __future__ import annotations

from typing import ClassVar

from oa_configurator import DatabaseConfig, PackageConfigBase, ResourceSpec


class OrmLoaderConfig(PackageConfigBase):
    """oa-configurator config class for orm-loader.

    orm-loader is connection-agnostic — it accepts SQLAlchemy sessions/engines
    as parameters and owns no database resources. This class exists to register
    orm-loader in the oa-configurator ecosystem, provide a canonical
    ``configure_logging()`` entry point, and declare the test database resource
    used by the integration test suite.
    """

    TEST_DB: ClassVar[ResourceSpec] = ResourceSpec(
        semantic_name="test_orm_db",
        display_name="ORM Loader Test Database",
        description="PostgreSQL database for running orm-loader integration tests.",
        connection_name_hint="pg_test_orm",
        is_cdm_database=False,
        cdm_schema_default="public",
        connection_defaults=DatabaseConfig(
            dialect="postgresql+psycopg",
            host="localhost",
            port=55432,
            user="test",
            password="test",
            database_name="test",
        ),
    )

    tool_name: ClassVar[str] = "orm_loader"
    extra_logging_namespaces: ClassVar[tuple[str, ...]] = ()
    test_resources: ClassVar[tuple[ResourceSpec, ...]] = (TEST_DB,)
