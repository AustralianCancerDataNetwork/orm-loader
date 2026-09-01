from __future__ import annotations

import sqlalchemy.event as sae
from typing import TYPE_CHECKING, Type, cast

import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine

from orm_loader.backends import STAGING_SCHEMA, Dialect, PostgresBackend
from orm_loader.helpers.sql import qualify_identifier
from tests.models import ComputedColumnTable

_TARGET_TABLE = ComputedColumnTable.__tablename__
_STAGING_TABLE = f"_staging_{_TARGET_TABLE}"
_PREPARER = postgresql.dialect().identifier_preparer
_STAGING_TABLE_WITH_SCHEMA: str = qualify_identifier(_STAGING_TABLE, STAGING_SCHEMA, _PREPARER)

_ComputedTableCls = cast("Type[CSVTableProtocol]", ComputedColumnTable)


if TYPE_CHECKING:
    from orm_loader.tables.typing import CSVTableProtocol


class _FakeSession:
    def __init__(self, scalar_result: str | int = "origin") -> None:
        self.statements: list[str] = []
        self.scalar_result = scalar_result
        self.commits = 0

    def execute(self, statement, parameters=None):
        if hasattr(statement, "compile"):
            sql = str(statement.compile(dialect=postgresql.dialect()))
        else:
            sql = str(statement)
        self.statements.append(sql)

        class _Result:
            def __init__(self, value):
                self._value = value

            def scalar(self):
                return self._value

            def scalar_one(self):
                return self._value

        return _Result(self.scalar_result)

    def commit(self) -> None:
        self.commits += 1


def _sess(s: _FakeSession) -> so.Session:
    return cast(so.Session, s)


def test_postgres_backend_identity_and_capabilities():
    backend = PostgresBackend()

    assert backend.name == "postgres"
    assert backend.dialect == Dialect.POSTGRESQL
    assert backend.supports_dialect(Dialect.POSTGRESQL) is True
    assert backend.capabilities.supports_fast_load is True
    assert backend.capabilities.supports_unlogged_staging is True
    assert backend.capabilities.supports_fk_toggle is True
    assert backend.capabilities.supports_materialized_views is True


def test_qualify_identifier_escapes_embedded_quotes():
    assert qualify_identifier("table", 'schema"name', _PREPARER) == '"schema""name"."table"'
    assert qualify_identifier('ta"ble', None, _PREPARER) == '"ta""ble"'


def test_postgres_backend_default_staging_schema_is_none():
    backend = PostgresBackend()

    assert backend.staging_schema is None
    assert backend.qualified_staging_name(_TARGET_TABLE) == _PREPARER.quote_identifier(_STAGING_TABLE)


def test_postgres_backend_create_staging_table_drops_computed_columns(pg_session):
    backend = PostgresBackend(staging_schema=STAGING_SCHEMA)

    backend.create_staging_table(_ComputedTableCls, pg_session)

    inspector = sa.inspect(pg_session.get_bind())
    cols = {c["name"] for c in inspector.get_columns(_STAGING_TABLE, schema=STAGING_SCHEMA)}
    assert cols == {"id", "name", "_rownum"}  # slug is computed, excluded


def test_postgres_backend_drop_staging_table():
    backend = PostgresBackend(staging_schema=STAGING_SCHEMA)
    session = _FakeSession()

    backend.drop_staging_table(_ComputedTableCls, _sess(session))

    assert session.statements == [f'DROP TABLE IF EXISTS {_STAGING_TABLE_WITH_SCHEMA}']


def test_postgres_backend_fk_methods_emit_expected_sql():
    backend = PostgresBackend()
    session = _FakeSession()

    previous = backend.disable_fk_check(_sess(session))
    enabled = backend.enable_fk_check(_sess(session))
    backend.restore_fk_check(_sess(session), previous)

    assert previous == "origin"
    assert enabled == "origin"
    assert session.statements == [
        "SHOW session_replication_role",
        "SET session_replication_role = 'replica'",
        "SHOW session_replication_role",
        "SET session_replication_role = 'origin'",
        "SET session_replication_role = 'origin'",
    ]


def test_postgres_backend_materialized_view_methods_work_end_to_end(pg_db):
    """Real create + refresh + query, not just checking emitted SQL text.
    The whole point is proving this DDL actually round-trips correctly."""
    backend = PostgresBackend()
    conn = pg_db.connection
    selectable = sa.select(sa.literal(1).label("n"))

    backend.create_materialized_view(conn, "mv_test", selectable)
    backend.refresh_materialized_view(conn, "mv_test")

    assert conn.execute(sa.text("SELECT n FROM mv_test")).scalar() == 1


def test_postgres_backend_normalize_fk_check_state():
    normalize = PostgresBackend._normalize_fk_check_state

    assert normalize("origin") == "origin"
    assert normalize("local") == "local"
    assert normalize("replica") == "replica"
    assert normalize(" ORIGIN ") == "origin"

    try:
        normalize("invalid_role")
    except ValueError as exc:
        assert "Invalid PostgreSQL session_replication_role" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unrecognised role")

    try:
        normalize(1)
    except ValueError as exc:
        assert "Postgres uses string roles" in str(exc)
    else:
        raise AssertionError("Expected ValueError for integer input")


def test_postgres_backend_disable_fk_raises_when_show_returns_non_string():
    backend = PostgresBackend()
    session = _FakeSession(scalar_result=42)

    try:
        backend.disable_fk_check(_sess(session))
    except RuntimeError as exc:
        assert "Expected PostgreSQL FK state to be a string" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError when SHOW returns a non-string")


def test_postgres_backend_enable_fk_raises_when_show_returns_non_string():
    backend = PostgresBackend()
    session = _FakeSession(scalar_result=42)

    try:
        backend.enable_fk_check(_sess(session))
    except RuntimeError as exc:
        assert "Expected PostgreSQL FK state to be a string" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError when SHOW returns a non-string")



def test_postgres_backend_engine_with_replica_role_unregisters_listener(monkeypatch):
    backend = PostgresBackend()
    events: list[tuple[str, object, str]] = []
    statements: list[str] = []

    class _Result:
        def scalar(self):
            return "origin"

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_) -> None:
            return None

        def execution_options(self, **_):
            return self

        def execute(self, statement):
            sql = str(statement.compile(dialect=postgresql.dialect()))
            statements.append(sql)
            return _Result()

    class _Engine:
        def connect(self):
            events.append(("connect", self, "connect"))
            return _Conn()

    engine = _Engine()

    def _listen(target, name, *_) -> None:
        events.append(("listen", target, name))

    def _remove(target, name, *_) -> None:
        events.append(("remove", target, name))

    monkeypatch.setattr(sae, "listen", _listen)
    monkeypatch.setattr(sae, "remove", _remove)

    with backend.engine_with_replica_role(cast(Engine, engine)):
        pass

    assert events == [
        ("listen", engine, "connect"),
        ("remove", engine, "connect"),
        ("connect", engine, "connect"),
    ]
    assert statements == [
        "SET session_replication_role = DEFAULT",
        "SHOW session_replication_role",
    ]
