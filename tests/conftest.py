from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so
from dotenv import load_dotenv

from oa_configurator import Role
from oa_configurator.testing import ScopedTestSchema, isolated_test_database, scoped_test_schema
from orm_loader.backends import staging_schema_claim
from orm_loader.config import OrmLoaderConfig
from tests.models import Base

load_dotenv(Path(__file__).parent.parent / ".env")


@pytest.fixture
def engine():
    with isolated_test_database(
        OrmLoaderConfig, "test_orm_db_sqlite", dialect="sqlite",
    ) as db:
        engine = db.connection.engine
        Base.metadata.create_all(engine)
        yield engine


@pytest.fixture
def session(engine):
    with so.Session(engine) as s:
        yield s


# ---------------------------------------------------------------------------
# Postgres fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def pg_db(request):
    """Isolated PostgreSQL test database. Everything done through
    ``pg_db.connection``/``pg_db.session`` happens inside one transaction
    that's rolled back on exit, so concurrent test runs can't collide and
    nothing needs manual cleanup."""
    with isolated_test_database(
        OrmLoaderConfig, "test_orm_db_pg", request=request, schema_claims=[staging_schema_claim()],
    ) as db:
        yield db


@pytest.fixture
def pg_session(pg_db):
    """The standard fixture for tests needing real tables ready to query:
    creates the staging schema and Base.metadata inside pg_db's already-open,
    rolled-back transaction, then returns pg_db.session."""
    conn = pg_db.connection
    Base.metadata.create_all(conn)
    return pg_db.session


@contextmanager
def schema_scoped_session(
    pg_db, table: sa.Table, *, prefix: str, split_roles: Iterable[Role] = ()
) -> Iterator[tuple[ScopedTestSchema, so.Session]]:
    """A Session on committed scoped test schemas, with the staging schema
    claimed and *table* already created."""
    with scoped_test_schema(
        pg_db.resolved, prefix=prefix, split_roles=split_roles, schema_claims=[staging_schema_claim()]
    ) as scoped:
        with scoped.engine.begin() as conn:
            table.create(conn, checkfirst=True)
        with so.Session(scoped.engine) as session:
            yield scoped, session
