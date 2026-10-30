from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so
from dotenv import load_dotenv

from oa_configurator import Dialect, Role
from oa_configurator.testing import DIALECT_PARAMS, ScopedTestSchema, isolated_test_database, scoped_test_schema
from orm_loader.backends import staging_schema_claim
from orm_loader.config import OrmLoaderConfig
from tests.models import Base

load_dotenv(Path(__file__).parent.parent / ".env")


@pytest.fixture
def sqlite_engine():
    with isolated_test_database(
        OrmLoaderConfig, "test_orm_db_sqlite", dialect="sqlite",
    ) as db:
        sqlite_engine = db.connection.engine
        Base.metadata.create_all(sqlite_engine)
        yield sqlite_engine


@pytest.fixture
def sqlite_session(sqlite_engine):
    with so.Session(sqlite_engine) as s:
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


# ---------------------------------------------------------------------------
# DuckDB fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def duckdb_db(request):
    """Isolated DuckDB test database, a fresh disposable tempfile per test.
    Schema-claimed the same way as pg_db: DuckDB genuinely supports schemas,
    unlike SQLite."""
    with isolated_test_database(
        OrmLoaderConfig, "test_orm_db_duckdb", dialect="duckdb", request=request,
        schema_claims=[staging_schema_claim()],
    ) as db:
        yield db


@pytest.fixture
def duckdb_session(duckdb_db):
    """The standard fixture for tests needing real tables ready to query:
    creates the staging schema and Base.metadata, then returns duckdb_db.session."""
    conn = duckdb_db.connection
    Base.metadata.create_all(conn)
    conn.commit()
    return duckdb_db.session


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
        with so.Session(scoped.engine) as sqlite_session:
            yield scoped, sqlite_session


# ---------------------------------------------------------------------------
# One session per supported dialect
# ---------------------------------------------------------------------------

_SESSION_FIXTURE_BY_DIALECT = {
    Dialect.SQLITE: "sqlite_session",
    Dialect.POSTGRESQL: "pg_session",
    Dialect.DUCKDB: "duckdb_session",
}
assert set(_SESSION_FIXTURE_BY_DIALECT) == {param.values[0] for param in DIALECT_PARAMS}, (
    "_SESSION_FIXTURE_BY_DIALECT is missing an entry for a dialect DIALECT_PARAMS now covers."
)


def session_for(request: pytest.FixtureRequest, dialect: Dialect) -> so.Session:
    """Return the session fixture of one dialect.

    The lookup is dynamic, so the dialect's own mark and ``forked`` come from
    ``DIALECT_PARAMS`` on the parametrised value rather than from fixture names.
    """
    return request.getfixturevalue(_SESSION_FIXTURE_BY_DIALECT[dialect])


@pytest.fixture(params=DIALECT_PARAMS)
def dialect_session(request: pytest.FixtureRequest) -> so.Session:
    """A session on each supported dialect in turn."""
    return session_for(request, request.param)
