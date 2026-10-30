from __future__ import annotations

from typing import TYPE_CHECKING, Type, cast

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.dialects import sqlite as sqlite_dialect

from orm_loader.backends import Dialect, SQLiteBackend
from tests.models import ComputedColumnTable, ReferencingChildTable, SimpleTable

if TYPE_CHECKING:
    from orm_loader.tables.typing import FileTableProtocol

_TARGET_TABLE = ComputedColumnTable.__tablename__
_STAGING_TABLE = f"_staging_{_TARGET_TABLE}"


class _FakeSession:
    def __init__(self, *scalar_results: int | str) -> None:
        self.statements: list[str] = []
        self.scalar_results = list(scalar_results) or [1]

    def execute(self, statement, **kwargs):
        if hasattr(statement, "compile"):
            sql = str(statement.compile(dialect=sqlite_dialect.dialect())).strip()
        else:
            sql = str(statement)
        self.statements.append(sql)

        class _Result:
            def __init__(self, value):
                self._value = value

            def scalar(self):
                return self._value

        return _Result(self.scalar_results.pop(0) if len(self.scalar_results) > 1 else self.scalar_results[0])


_ComputedTableCls = cast("Type[FileTableProtocol]", ComputedColumnTable)


def _sess(s: _FakeSession) -> so.Session:
    return cast(so.Session, s)


def test_sqlite_backend_identity_and_capabilities():
    backend = SQLiteBackend()

    assert backend.name == "sqlite"
    assert backend.dialect == Dialect.SQLITE
    assert backend.supports_dialect(Dialect.SQLITE) is True
    assert backend.capabilities.supports_unlogged_staging is False
    assert backend.capabilities.supports_fk_toggle is True
    assert backend.capabilities.supports_materialized_views is False
    assert backend.resolve_index_strategy("auto") == "keep"


@pytest.mark.parametrize("merge_batch_size", [0, -1])
@pytest.mark.parametrize("merge_strategy", ["replace", "upsert", "insert"])
def test_sqlite_backend_rejects_non_positive_merge_batch_size(merge_batch_size, merge_strategy):
    backend = SQLiteBackend()
    sqlite_session = _sess(_FakeSession())
    merge = getattr(backend, f"merge_{merge_strategy}")
    args = [_ComputedTableCls, sqlite_session]
    if merge_strategy in {"replace", "upsert"}:
        args.append(["id"])

    with pytest.raises(ValueError, match="merge_batch_size must be a positive integer or None"):
        merge(*args, merge_batch_size=merge_batch_size)


def test_sqlite_backend_create_staging_table(sqlite_session, sqlite_engine):
    backend = SQLiteBackend()

    backend.create_staging_table(_ComputedTableCls, sqlite_session)
    inspector = sa.inspect(sqlite_engine)
    assert inspector.has_table(_STAGING_TABLE) is True
    cols = inspector.get_columns(_STAGING_TABLE)
    assert [c["name"] for c in cols] == ["id", "name"]
    target = {c.name: c.nullable for c in ComputedColumnTable.__table__.columns}
    assert {c["name"]: c["nullable"] for c in cols} == {name: target[name] for name in ("id", "name")}


def test_sqlite_backend_disable_fk_reads_sets_then_reads_back():
    backend = SQLiteBackend()
    sqlite_session = _FakeSession(1, 0)

    previous = backend.disable_fk_check(_sess(sqlite_session))

    assert previous == 1
    assert sqlite_session.statements == [
        "PRAGMA foreign_keys",
        "PRAGMA foreign_keys = OFF",
        "PRAGMA foreign_keys",
    ]


def test_sqlite_backend_disable_fk_raises_when_the_pragma_is_ignored():
    backend = SQLiteBackend()

    with pytest.raises(RuntimeError, match="inside a transaction"):
        backend.disable_fk_check(_sess(_FakeSession(1, 1)))


def test_sqlite_backend_restore_fk_normalises_int_and_emits():
    backend = SQLiteBackend()
    sqlite_session = _FakeSession()

    backend.restore_fk_check(_sess(sqlite_session), 1)
    backend.restore_fk_check(_sess(sqlite_session), 0)

    assert sqlite_session.statements == [
        "PRAGMA foreign_keys = ON",
        "PRAGMA foreign_keys = OFF",
    ]


def test_sqlite_backend_normalize_fk_check_state():
    normalize = SQLiteBackend._normalize_fk_check_state

    assert normalize(1) == "ON"
    assert normalize(0) == "OFF"
    assert normalize("1") == "ON"
    assert normalize("0") == "OFF"
    assert normalize("ON") == "ON"
    assert normalize("OFF") == "OFF"
    assert normalize("on") == "ON"
    assert normalize("off") == "OFF"

    try:
        normalize(2)
    except ValueError as exc:
        assert "Invalid SQLite foreign_keys state" in str(exc)
    else:
        raise AssertionError("Expected ValueError for out-of-range int")

    try:
        normalize("enabled")
    except ValueError as exc:
        assert "Invalid SQLite foreign_keys state" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unrecognised string")


def test_sqlite_backend_materialized_view_methods_raise(sqlite_engine):
    backend = SQLiteBackend()
    selectable = sa.select(sa.literal(1).label("n"))

    try:
        backend.create_materialized_view(sqlite_engine, "mv_test", selectable)
    except NotImplementedError as exc:
        assert "does not support materialized views" in str(exc)
    else:
        raise AssertionError("Expected create_materialized_view() to raise NotImplementedError")

    try:
        backend.refresh_materialized_view(sqlite_engine, "mv_test")
    except NotImplementedError as exc:
        assert "does not support materialized views" in str(exc)
    else:
        raise AssertionError("Expected refresh_materialized_view() to raise NotImplementedError")


def test_sqlite_backend_drop_materialized_view_raises(sqlite_engine):
    backend = SQLiteBackend()

    with pytest.raises(NotImplementedError, match="does not support materialized views"):
        backend.drop_materialized_view(sqlite_engine, "mv_test")


def test_sqlite_backend_create_materialized_view_index_raises(sqlite_engine):
    from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex

    backend = SQLiteBackend()
    index = MaterializedViewIndex(name="mv_test_uq", columns=("id",), unique=True)

    with pytest.raises(NotImplementedError, match="does not support materialized views"):
        backend.create_materialized_view_index(sqlite_engine, "mv_test", index)


def test_sqlite_backend_disable_fk_raises_when_pragma_returns_non_int():
    backend = SQLiteBackend()
    sqlite_session = _FakeSession("not_an_int")

    try:
        backend.disable_fk_check(_sess(sqlite_session))
    except RuntimeError as exc:
        assert "Expected SQLite FK state to be an int" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError when PRAGMA returns a non-int")


def test_sqlite_backend_restore_fk_accepts_string_values():
    backend = SQLiteBackend()
    sqlite_session = _FakeSession()

    backend.restore_fk_check(_sess(sqlite_session), "ON")
    backend.restore_fk_check(_sess(sqlite_session), "OFF")

    assert sqlite_session.statements == [
        "PRAGMA foreign_keys = ON",
        "PRAGMA foreign_keys = OFF",
    ]


def test_sqlite_backend_fk_toggle_round_trip(sqlite_session):
    backend = SQLiteBackend()

    sqlite_session.execute(sa.text("PRAGMA foreign_keys = ON"))
    assert sqlite_session.execute(sa.text("PRAGMA foreign_keys")).scalar() == 1

    previous = backend.disable_fk_check(sqlite_session)
    assert sqlite_session.execute(sa.text("PRAGMA foreign_keys")).scalar() == 0

    backend.restore_fk_check(sqlite_session, previous)
    assert sqlite_session.execute(sa.text("PRAGMA foreign_keys")).scalar() == 1


def test_relaxed_foreign_keys_on_sqlite_logs_the_toggling(sqlite_session, caplog):
    with caplog.at_level("INFO", logger="orm_loader.backends.base"):
        with SQLiteBackend().relaxed_foreign_keys(sqlite_session):
            pass

    assert "Foreign key checks disabled" in caplog.text
    assert "Foreign key checks restored" in caplog.text


@pytest.fixture
def enforcing_session(sqlite_engine):
    """A session whose connections come up with foreign key enforcement on."""
    sa.event.listen(sqlite_engine, "connect", lambda conn, _: conn.execute("PRAGMA foreign_keys = ON"))
    sqlite_engine.dispose()
    with so.Session(sqlite_engine) as session:
        yield session


def _foreign_keys(session: so.Session) -> int:
    return session.execute(sa.text("PRAGMA foreign_keys")).scalar_one()


def test_a_load_relaxes_enforced_foreign_keys_and_restores_them(enforcing_session, tmp_path):
    path = tmp_path / "referencing_child_table.csv"
    path.write_text("id,parent_id\n1,1\n")
    child = cast("Type[FileTableProtocol]", ReferencingChildTable)

    assert child.load_file(enforcing_session, path, staging_schema_tag=None) == 1

    assert _foreign_keys(enforcing_session) == 1


def test_a_load_on_a_session_with_pending_writes_raises_and_keeps_them(enforcing_session, tmp_path):
    enforcing_session.add(SimpleTable(id=1, name="pending"))
    enforcing_session.flush()
    path = tmp_path / f"{SimpleTable.__tablename__}.csv"
    path.write_text("id,name\n2,a\n")
    simple = cast("Type[FileTableProtocol]", SimpleTable)

    with pytest.raises(RuntimeError, match="inside a transaction"):
        simple.load_file(enforcing_session, path, staging_schema_tag=None)

    assert enforcing_session.get(SimpleTable, 1) is not None
