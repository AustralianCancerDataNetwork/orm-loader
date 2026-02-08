import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so
import os
from orm_loader.tables import CSVLoadableTableInterface
import time

from tests.models import Base

@pytest.fixture
def engine():
    return sa.create_engine("sqlite:///:memory:")

@pytest.fixture
def session(engine):
    Base.metadata.create_all(engine)
    with so.Session(engine) as s:
        yield s


POSTGRES_URL = "postgresql+psycopg2://test:test@localhost:55432/test_db"

@pytest.fixture(scope="session")
def pg_engine():
    last_err = None
    for i in range(20):
        try:
            engine = sa.create_engine(POSTGRES_URL, future=True)
            with engine.connect() as conn:
                conn.execute(sa.text("select 1"))
            print("Postgres connection established")
            yield engine
            engine.dispose()
            return
        except Exception as e:
            last_err = e
            print(f"[{i}] Postgres not ready:", repr(e))
            time.sleep(1)

    raise RuntimeError(f"Postgres never became available: {last_err!r}")

@pytest.fixture
def pg_session(pg_engine):
    Session = so.sessionmaker(bind=pg_engine, future=True)
    with pg_engine.begin() as conn:
        # optional: recreate schema per test
        Base.metadata.drop_all(conn)
        Base.metadata.create_all(conn)

    session = Session()
    try:
        yield session
    finally:
        session.rollback()
        session.close()




