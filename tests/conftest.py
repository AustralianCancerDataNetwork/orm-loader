import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so
import os
from orm_loader.tables import CSVLoadableTableInterface

from tests.models import Base

@pytest.fixture
def engine():
    return sa.create_engine("sqlite:///:memory:")

@pytest.fixture
def session(engine):
    Base.metadata.create_all(engine)
    with so.Session(engine) as s:
        yield s

POSTGRES_DSN_ENV = "ORM_LOADER_TEST_PG_DSN"

@pytest.fixture(scope="session")
def pg_engine():
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(
            f"Postgres tests skipped: {POSTGRES_DSN_ENV} not set",
            allow_module_level=True,
        )

    engine = sa.create_engine(dsn, future=True)

    # fresh schema for test run
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    yield engine

    engine.dispose()


@pytest.fixture
def pg_session(pg_engine):
    with so.Session(pg_engine) as session:
        yield session
        session.rollback()




