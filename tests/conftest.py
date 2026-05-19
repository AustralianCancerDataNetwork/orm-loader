import os
import time
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so
from dotenv import load_dotenv

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

POSTGRES_URL = os.getenv(
    "TEST_POSTGRES_URL",
    "postgresql+psycopg://test:test@localhost:55432/test",
)

# Shown whenever Postgres is unreachable — centralised so every skip carries
# the same actionable instructions.
_PG_SKIP_MSG = (
    "Postgres tests skipped — could not connect to {url}.\n"
    "  Set TEST_POSTGRES_URL to a writable test database and re-run, e.g.:\n"
    "    export TEST_POSTGRES_URL='postgresql+psycopg://user:pass@host:5432/orm_loader_test'\n"
    "  Or add it to orm-loader/.env.\n"
    "  Last error: {{last_err}}"
).format(url=POSTGRES_URL)

# Module-level sentinel: None = not yet attempted, str = skip reason.
# Prevents the 20-retry loop from running once per postgres test when
# the server is not reachable.
_pg_unavailable: str | None = None


def _ensure_db_exists(url: str) -> None:
    """Create the target database if it doesn't already exist.

    Connects to the 'postgres' maintenance database (same host/user/pass)
    so the target database can be created without touching anything else.
    """
    parsed = urlparse(url)
    db_name = parsed.path.lstrip("/")
    admin_url = urlunparse(parsed._replace(path="/postgres"))

    admin_engine = sa.create_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        with admin_engine.connect() as conn:
            exists = conn.execute(
                sa.text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": db_name},
            ).scalar()
            if not exists:
                conn.execute(sa.text(f'CREATE DATABASE "{db_name}"'))
                print(f"Created test database: {db_name!r}")
    finally:
        admin_engine.dispose()


@pytest.fixture(scope="session")
def pg_engine():
    global _pg_unavailable

    # Fast path: already know Postgres is not reachable — skip immediately
    # without re-running the retry loop.
    if _pg_unavailable is not None:
        pytest.skip(_pg_unavailable)

    try:
        _ensure_db_exists(POSTGRES_URL)
    except Exception as e:
        print(f"Could not ensure test DB exists (will try connecting anyway): {e}")

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

    _pg_unavailable = _PG_SKIP_MSG.format(last_err=last_err)
    pytest.skip(_pg_unavailable)


@pytest.fixture
def pg_session(pg_engine):
    Session = so.sessionmaker(bind=pg_engine, future=True)
    with pg_engine.begin() as conn:
        Base.metadata.drop_all(conn)
        Base.metadata.create_all(conn)

    session = Session()
    try:
        yield session
    finally:
        session.rollback()
        session.close()
