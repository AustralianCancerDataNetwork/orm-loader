"""Configuration for orm-loader via oa-configurator."""

from __future__ import annotations

from typing import Annotated, ClassVar

from oa_configurator import CDMDatabaseConfig, PackageConfigBase, RefTo


class OrmLoaderConfig(PackageConfigBase):
    """oa-configurator config class for orm-loader.

    orm-loader is connection-agnostic — it accepts SQLAlchemy sessions/engines
    as parameters and owns no production database resource of its own. This
    class exists to register orm-loader in the oa-configurator ecosystem,
    provide a canonical ``configure_logging()`` entry point, and declare the
    test database used by the integration test suite.

    Notes
    -----
    By design, this config is for internal use only and must not be
    imported or resolved by any other package.
    """

    tool_name: ClassVar[str] = "orm_loader"
    extra_logging_namespaces: ClassVar[tuple[str, ...]] = ()

    test_orm_db: Annotated[str | None, RefTo(CDMDatabaseConfig, is_test=True)] = None
