from pathlib import Path

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so
from dotenv import load_dotenv

from orm_loader.backends import STAGING_SCHEMA
from tests.models import Base

load_dotenv(Path(__file__).parent.parent / ".env")


@pytest.fixture
def engine():
    engine = sa.create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def session(engine):
    with so.Session(engine) as s:
        yield s


# ---------------------------------------------------------------------------
# Postgres fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def pg_db():
    """Isolated PostgreSQL test database. Everything done through
    ``pg_db.connection``/``pg_db.session`` happens inside one transaction
    that's rolled back on exit, so concurrent test runs can't collide and
    nothing needs manual cleanup."""
    from oa_configurator.testing import isolated_test_database
    from orm_loader.config import OrmLoaderConfig

    with isolated_test_database(OrmLoaderConfig, "test_orm_db") as db:
        yield db


@pytest.fixture
def pg_session(pg_db):
    """The standard fixture for tests needing real tables ready to query:
    creates the staging schema and Base.metadata inside pg_db's already-open,
    rolled-back transaction, then returns pg_db.session."""
    conn = pg_db.connection
    conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA}"))
    Base.metadata.create_all(conn)
    return pg_db.session
