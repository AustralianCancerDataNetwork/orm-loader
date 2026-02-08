import os
import time
import pytest
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database

from tests.models import Base

POSTGRES_URL = "postgresql+psycopg://test:test@localhost:55432/test_db"

@pytest.fixture(scope="session")
def pg_engine():
    # wait for container
    for _ in range(20):
        try:
            engine = sa.create_engine(POSTGRES_URL, future=True)
            with engine.connect() as conn:
                conn.execute(sa.text("select 1"))
            break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("Postgres never became available")

    yield engine

    engine.dispose()


@pytest.fixture
def pg_session(pg_engine):
    Session = sessionmaker(bind=pg_engine, future=True)
    with pg_engine.begin() as conn:
        conn.execute(sa.text('DROP SCHEMA public CASCADE'))
        conn.execute(sa.text('CREATE SCHEMA public'))
        Base.metadata.drop_all(conn)
        Base.metadata.create_all(conn)

    session = Session()
    try:
        yield session
    finally:
        session.rollback()
        session.close()
