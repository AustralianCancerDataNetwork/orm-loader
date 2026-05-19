from __future__ import annotations

import sqlalchemy.event as sae
from typing import TYPE_CHECKING, Type, cast

import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection, Engine

from orm_loader.backends import Dialect, PostgresBackend

if TYPE_CHECKING:
    from orm_loader.tables.typing import CSVTableProtocol


class _ComputedTable:
    __table__ = sa.Table(
        "target_table",
        sa.MetaData(),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String),
        sa.Column("slug", sa.String, sa.Computed("lower(name)")),
    )


class _FakeSession:
    def __init__(self, scalar_result: str | int = "origin") -> None:
        self.statements: list[str] = []
        self.scalar_result = scalar_result
        self.commits = 0

    def execute(self, statement):
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

        return _Result(self.scalar_result)

    def commit(self) -> None:
        self.commits += 1


_ComputedTableCls = cast("Type[CSVTableProtocol]", _ComputedTable)


def _sess(s: _FakeSession) -> so.Session:
    return cast(so.Session, s)


def _as_engine(s: _FakeSession) -> Engine | Connection:
    return cast(Engine, s)


def test_postgres_backend_identity_and_capabilities():
    backend = PostgresBackend()

    assert backend.name == "postgres"
    assert backend.dialect == Dialect.POSTGRESQL
    assert backend.supports_dialect(Dialect.POSTGRESQL) is True
    assert backend.capabilities.supports_fast_load is True
    assert backend.capabilities.supports_unlogged_staging is True
    assert backend.capabilities.supports_fk_toggle is True
    assert backend.capabilities.supports_materialized_views is True


def test_postgres_backend_create_staging_table_drops_computed_columns():
    backend = PostgresBackend()
    session = _FakeSession()

    backend.create_staging_table(_ComputedTableCls, _sess(session), "_staging_target_table")

    assert any('DROP TABLE IF EXISTS "_staging_target_table"' in sql for sql in session.statements)
    assert any('CREATE UNLOGGED TABLE "_staging_target_table"' in sql for sql in session.statements)
    assert any('ALTER TABLE "_staging_target_table" DROP COLUMN "slug"' in sql for sql in session.statements)
    assert session.commits == 1


def test_postgres_backend_drop_staging_table():
    backend = PostgresBackend()
    session = _FakeSession()

    backend.drop_staging_table(_sess(session), "_staging_target_table")

    assert session.statements == ['DROP TABLE IF EXISTS "_staging_target_table"']


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


def test_postgres_backend_merge_replace_uses_using_delete():
    backend = PostgresBackend()
    session = _FakeSession()

    backend.merge_replace(_ComputedTableCls, _sess(session), "target_table", "_staging_target_table", ["id", "name"])

    sql = session.statements[0]
    assert 'DELETE FROM "target_table" t' in sql
    assert 'USING "_staging_target_table" s' in sql
    assert 't."id" = s."id" AND t."name" = s."name"' in sql


def test_postgres_backend_merge_insert_excludes_computed_columns():
    backend = PostgresBackend()
    session = _FakeSession()

    backend.merge_insert(_ComputedTableCls, _sess(session), "target_table", "_staging_target_table")

    sql = session.statements[0]
    assert 'INSERT INTO "target_table" ("id", "name")' in sql
    assert 'SELECT "id", "name" FROM "_staging_target_table"' in sql


def test_postgres_backend_merge_upsert_excludes_computed_columns():
    backend = PostgresBackend()
    session = _FakeSession()

    backend.merge_upsert(_ComputedTableCls, _sess(session), "target_table", "_staging_target_table", ["id"])

    sql = session.statements[0]
    assert 'INSERT INTO "target_table" ("id", "name")' in sql
    assert 'ON CONFLICT ("id") DO NOTHING' in sql


def test_postgres_backend_materialized_view_methods_emit_expected_sql():
    backend = PostgresBackend()
    session = _FakeSession()
    selectable = sa.select(sa.literal(1).label("n"))

    backend.create_materialized_view(_as_engine(session), "mv_test", selectable)
    backend.refresh_materialized_view(_as_engine(session), "mv_test")

    assert any("CREATE MATERIALIZED VIEW IF NOT EXISTS mv_test as SELECT" in sql for sql in session.statements)
    assert any("REFRESH MATERIALIZED VIEW mv_test;" == sql for sql in session.statements)


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
