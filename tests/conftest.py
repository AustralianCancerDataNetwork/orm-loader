import time
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

@pytest.fixture(scope="session")
def pg_engine():
    from oa_configurator.pytest_plugin import ensure_test_db_exists, resolve_test_resource
    from orm_loader.config import OrmLoaderConfig

    url = resolve_test_resource(OrmLoaderConfig.TEST_DB)

    try:
        ensure_test_db_exists(url)
    except Exception as exc:
        print(f"Could not ensure test DB exists, will try anyway: {exc}")

    last_err = None
    for i in range(20):
        engine: sa.Engine | None = None
        try:
            engine = sa.create_engine(url, future=True)
            with engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
            print("Postgres connection established")
            yield engine
            engine.dispose()
            return
        except Exception as exc:
            if engine is not None:
                engine.dispose()
            last_err = exc
            print(f"[{i}] Postgres not ready:", repr(exc))
            time.sleep(1)

    pytest.skip(f"PostgreSQL never became available: {last_err}")


@pytest.fixture
def pg_session(pg_engine):
    Session = so.sessionmaker(pg_engine, future=True)
    with pg_engine.begin() as conn:
        conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {STAGING_SCHEMA} CASCADE"))
        conn.execute(sa.text(f"CREATE SCHEMA {STAGING_SCHEMA}"))
        Base.metadata.drop_all(conn)
        Base.metadata.create_all(conn)

    session = Session()
    try:
        yield session
    finally:
        session.rollback()
        session.close()
