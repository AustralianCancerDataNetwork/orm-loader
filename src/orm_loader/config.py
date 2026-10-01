"""Configuration for orm-loader via oa-configurator."""

from __future__ import annotations

from typing import Annotated, ClassVar

from oa_configurator import CDMDatabaseConfig, PackageConfigBase, RefTo
from pydantic import Field


class OrmLoaderConfig(PackageConfigBase):
    """oa-configurator config class for orm-loader.

    orm-loader is connection-agnostic: it accepts SQLAlchemy sessions/engines
    as parameters and owns no production database resource of its own. This
    class exists to register orm-loader in the oa-configurator ecosystem,
    provide a canonical ``configure_logging()`` entry point, and declare the
    test database used by the integration test suite.

    Attributes
    ----------
    test_orm_db_pg : str, optional
        Name of the ``[databases.*]`` entry holding the test database. Must
        resolve to a real PostgreSQL connection; used for real integration
        testing of Postgres-only behavior.
    test_orm_db_sqlite : str, optional
        Same shape as ``test_orm_db_pg``, for tests that must always run
        against SQLite specifically, regardless of what ``test_orm_db_pg``
        happens to be configured to. Left unconfigured by design in every
        environment.

    Notes
    -----
    By design, this config is for internal use only and must not be
    imported or resolved by any other package.
    """

    tool_name: ClassVar[str] = "orm_loader"
    extra_logging_namespaces: ClassVar[tuple[str, ...]] = ()

    test_orm_db_pg: Annotated[str | None, RefTo(CDMDatabaseConfig, is_test=True)] = Field(
        default=None,
        description="Real PostgreSQL test database, for Postgres-only integration testing.",
    )
    test_orm_db_sqlite: Annotated[str | None, RefTo(CDMDatabaseConfig, is_test=True)] = Field(
        default=None,
        description=(
            "Disposable SQLite test database; left unconfigured by design "
            "(isolated_test_database(..., dialect='sqlite') provisions one automatically)."
        ),
    )
