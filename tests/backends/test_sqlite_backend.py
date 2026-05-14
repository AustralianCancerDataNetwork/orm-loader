from __future__ import annotations

import sqlite3
from pathlib import Path

import sqlalchemy as sa
import sqlalchemy.orm as so

from orm_loader.backends import SQLiteBackend
from orm_loader.helpers.sqlite import attach_sqlite_bulk_load_pragmas


class _ComputedTable:
    __table__ = sa.Table(
        "target_table",
        sa.MetaData(),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String),
        sa.Column("slug", sa.String, sa.Computed("lower(name)")),
    )


class _FakeSession:
    def __init__(self, scalar_result=1) -> None:
        self.statements: list[str] = []
        self.scalar_result = scalar_result

    def execute(self, statement):
        self.statements.append(str(statement))

        class _Result:
            def __init__(self, value):
                self._value = value

            def scalar(self):
                return self._value

        return _Result(self.scalar_result)


def test_sqlite_backend_identity_and_capabilities():
    backend = SQLiteBackend()

    assert backend.name == "sqlite"
    assert backend.supports_dialect("sqlite") is True
    assert backend.capabilities.supports_fast_load is False
    assert backend.capabilities.supports_unlogged_staging is False
    assert backend.capabilities.supports_fk_toggle is True
    assert backend.capabilities.supports_materialized_views is False
    assert backend.resolve_index_strategy("auto") == "keep"
    assert backend.journal_mode == "WAL"


def test_sqlite_backend_create_staging_table():
    backend = SQLiteBackend()
    engine = sa.create_engine("sqlite:///:memory:", future=True)
    session = so.Session(engine)

    try:
        backend.create_staging_table(_ComputedTable, session, "_staging_target_table")
        inspector = sa.inspect(engine)
        assert inspector.has_table("_staging_target_table") is True
        cols = inspector.get_columns("_staging_target_table")
        assert [c["name"] for c in cols] == ["id", "name", "slug"]
        assert all(c["nullable"] is True for c in cols)
    finally:
        session.close()


def test_sqlite_backend_drop_staging_table():
    backend = SQLiteBackend()
    session = _FakeSession()

    backend.drop_staging_table(session, "_staging_target_table")

    assert session.statements == ['DROP TABLE IF EXISTS "_staging_target_table"']


def test_sqlite_backend_fk_methods_emit_expected_sql():
    backend = SQLiteBackend()
    session = _FakeSession()

    previous = backend.disable_fk_check(session)
    enabled = backend.enable_fk_check(session)
    backend.restore_fk_check(session, previous)

    assert previous == 1
    assert enabled == 1
    assert session.statements == [
        "PRAGMA foreign_keys",
        "PRAGMA foreign_keys = OFF",
        "PRAGMA foreign_keys",
        "PRAGMA foreign_keys = ON",
        "PRAGMA foreign_keys = 1",
    ]


def test_sqlite_backend_merge_replace_single_pk():
    backend = SQLiteBackend()
    session = _FakeSession()

    backend.merge_replace(_ComputedTable, session, "target_table", "_staging_target_table", ["id"])

    sql = session.statements[0]
    assert 'DELETE FROM "target_table"' in sql
    assert 'SELECT "id" FROM "_staging_target_table"' in sql


def test_sqlite_backend_merge_replace_composite_pk():
    backend = SQLiteBackend()
    session = _FakeSession()

    backend.merge_replace(_ComputedTable, session, "target_table", "_staging_target_table", ["id", "name"])

    sql = session.statements[0]
    assert 'WHERE EXISTS (' in sql
    assert '"target_table"."id" = "_staging_target_table"."id"' in sql
    assert '"target_table"."name" = "_staging_target_table"."name"' in sql


def test_sqlite_backend_merge_insert_excludes_computed_columns():
    backend = SQLiteBackend()
    session = _FakeSession()

    backend.merge_insert(_ComputedTable, session, "target_table", "_staging_target_table")

    sql = session.statements[0]
    assert 'INSERT INTO "target_table" ("id", "name")' in sql
    assert 'SELECT "id", "name" FROM "_staging_target_table"' in sql


def test_sqlite_backend_merge_upsert_excludes_computed_columns():
    backend = SQLiteBackend()
    session = _FakeSession()

    backend.merge_upsert(_ComputedTable, session, "target_table", "_staging_target_table", ["id"])

    sql = session.statements[0]
    assert 'INSERT OR IGNORE INTO "target_table" ("id", "name")' in sql


def test_sqlite_backend_materialized_view_methods_raise():
    backend = SQLiteBackend()
    session = _FakeSession()
    selectable = sa.select(sa.literal(1).label("n"))

    try:
        backend.create_materialized_view(session, "mv_test", selectable)
    except NotImplementedError as exc:
        assert "does not support materialized views" in str(exc)
    else:
        raise AssertionError("Expected create_materialized_view() to raise NotImplementedError")

    try:
        backend.refresh_materialized_view(session, "mv_test")
    except NotImplementedError as exc:
        assert "does not support materialized views" in str(exc)
    else:
        raise AssertionError("Expected refresh_materialized_view() to raise NotImplementedError")


def test_sqlite_backend_configures_bulk_load_pragmas(tmp_path: Path):
    backend = SQLiteBackend()
    db_path = tmp_path / "test.db"
    engine = sa.create_engine(f"sqlite:///{db_path}", future=True)
    backend.install_engine_hooks(engine)

    with engine.connect() as conn:
        busy_timeout = conn.execute(sa.text("PRAGMA busy_timeout")).scalar_one()
        journal_mode = conn.execute(sa.text("PRAGMA journal_mode")).scalar_one()
        foreign_keys = conn.execute(sa.text("PRAGMA foreign_keys")).scalar_one()

    assert busy_timeout == 60000
    assert str(journal_mode).lower() == "wal"
    assert foreign_keys == 1


def test_sqlite_backend_restore_journal_mode(tmp_path: Path):
    backend = SQLiteBackend()
    db_path = tmp_path / "journal.db"
    engine = sa.create_engine(f"sqlite:///{db_path}", future=True)
    backend.install_engine_hooks(engine)

    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(sa.text("INSERT INTO t (name) VALUES ('x')"))

    engine.dispose()
    backend.restore_journal_mode(db_path)

    with sqlite3.connect(db_path.resolve()) as conn:
        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]

    assert str(journal_mode).lower() == "delete"


def test_attach_sqlite_bulk_load_pragmas_installs_backend_hook(tmp_path: Path):
    db_path = tmp_path / "attached.db"
    engine = sa.create_engine(f"sqlite:///{db_path}", future=True)

    attach_sqlite_bulk_load_pragmas(engine, busy_timeout_ms=45000)

    with engine.connect() as conn:
        busy_timeout = conn.execute(sa.text("PRAGMA busy_timeout")).scalar_one()
        journal_mode = conn.execute(sa.text("PRAGMA journal_mode")).scalar_one()
        foreign_keys = conn.execute(sa.text("PRAGMA foreign_keys")).scalar_one()

    assert busy_timeout == 45000
    assert str(journal_mode).lower() == "wal"
    assert foreign_keys == 1


def test_sqlite_backend_rejects_invalid_journal_mode():
    try:
        SQLiteBackend(journal_mode="wal; drop table x;")
    except ValueError as exc:
        assert "Unsupported SQLite journal_mode" in str(exc)
    else:
        raise AssertionError("Expected invalid journal_mode to raise ValueError")
