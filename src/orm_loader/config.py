"""Configuration for orm-loader via oa-configurator."""

from __future__ import annotations

from typing import ClassVar, Final

from oa_configurator import PackageConfigBase

TOOL_NAME: Final[str] = "orm_loader"


class OrmLoaderConfig(PackageConfigBase):
    """oa-configurator config class for orm-loader.

    orm-loader is connection-agnostic — it accepts SQLAlchemy sessions/engines
    as parameters and owns no database resources. This class exists solely to
    register orm-loader in the oa-configurator ecosystem and to provide a
    canonical ``configure_logging()`` entry point.
    """

    tool_name: ClassVar[str] = TOOL_NAME
    extra_logging_namespaces: ClassVar[tuple[str, ...]] = ()
