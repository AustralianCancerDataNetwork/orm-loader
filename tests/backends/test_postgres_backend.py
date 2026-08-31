from __future__ import annotations

import sqlalchemy.event as sae
from typing import TYPE_CHECKING, Type, cast

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, ProgrammingError

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
    dialect = postgresql.dialect()

    def __init__(
        self,
        scalar_result: str | int | bool = "origin",
        *,
        raise_on_execute: Exception | None = None,
    ) -> None:
        self.statements: list[str] = []
        self.scalar_result = scalar_result
        self.raise_on_execute = raise_on_execute
        self.commits = 0

    def execute(self, statement, parameters=None):
        if hasattr(statement, "compile"):
            sql = str(statement.compile(dialect=postgresql.dialect()))
        else:
            sql = str(statement)
        self.statements.append(sql)
        if self.raise_on_execute is not None:
            raise self.raise_on_execute

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


def _conn(s: _FakeSession) -> sa.Connection:
    return cast(sa.Connection, s)


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

    created = backend.create_materialized_view(conn, "mv_test", selectable)
    refreshed = backend.refresh_materialized_view(conn, "mv_test")

    assert conn.execute(sa.text("SELECT n FROM mv_test")).scalar() == 1
    assert created.operation.value == "create"
    assert refreshed.operation.value == "refresh"


def test_postgres_backend_create_materialized_view_index_emits_expected_sql():
    from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex

    backend = PostgresBackend()
    session = _FakeSession()
    index = MaterializedViewIndex(name="mv_test_row_id_uq", columns=("row_id",), unique=True)

    backend.create_materialized_view_index(_conn(session), "mv_test", index, schema="reporting")

    assert session.statements == [
        'CREATE UNIQUE INDEX "mv_test_row_id_uq" '
        'ON "reporting"."mv_test" ("row_id")'
    ]


def test_postgres_backend_drop_materialized_view_default_args():
    backend = PostgresBackend()
    session = _FakeSession()

    backend.drop_materialized_view(_conn(session), "mv_test", schema="reporting")

    assert session.statements == [
        'DROP MATERIALIZED VIEW IF EXISTS "reporting"."mv_test"'
    ]


def test_postgres_backend_drop_materialized_view_cascade_and_if_exists_false():
    backend = PostgresBackend()
    session = _FakeSession()

    backend.drop_materialized_view(
        _conn(session), "mv_test", schema="reporting", if_exists=False, cascade=True
    )

    assert session.statements == [
        'DROP MATERIALIZED VIEW "reporting"."mv_test" CASCADE'
    ]


def test_postgres_backend_create_failure_preserves_cause():
    from orm_loader.backends.materialized_view_errors import (
        MaterializationError,
        MaterializationOperation,
    )

    original = RuntimeError("boom")
    backend = PostgresBackend()
    session = _FakeSession(raise_on_execute=original)

    with pytest.raises(MaterializationError) as exc_info:
        backend.create_materialized_view(
            _conn(session),
            "mv_test",
            sa.select(sa.literal(1).label("row_id")),
            schema="reporting",
        )

    assert exc_info.value.__cause__ is original
    assert exc_info.value.failure.operation is MaterializationOperation.CREATE


def test_postgres_backend_drop_materialized_view_failure_preserves_cause():
    from orm_loader.backends.materialized_view_errors import (
        MaterializationError,
        MaterializationOperation,
    )

    original = RuntimeError("boom")
    backend = PostgresBackend()
    session = _FakeSession(raise_on_execute=original)

    with pytest.raises(MaterializationError) as exc_info:
        backend.drop_materialized_view(_conn(session), "mv_test", schema="reporting")

    assert exc_info.value.__cause__ is original
    assert exc_info.value.failure.cause is original
    assert exc_info.value.failure.operation is MaterializationOperation.DROP


def test_postgres_backend_index_failure_preserves_cause_and_index_name():
    from orm_loader.backends.materialized_view_errors import (
        MaterializationError,
        MaterializationOperation,
    )
    from orm_loader.materialized_views import MaterializedViewIndex

    original = RuntimeError("boom")
    backend = PostgresBackend()
    session = _FakeSession(raise_on_execute=original)
    index = MaterializedViewIndex(name="mv_test_uq", columns=("row_id",), unique=True)

    with pytest.raises(MaterializationError) as exc_info:
        backend.create_materialized_view_index(
            _conn(session), "mv_test", index, schema="reporting"
        )

    assert exc_info.value.__cause__ is original
    assert exc_info.value.failure.operation is MaterializationOperation.CREATE_INDEX
    assert exc_info.value.failure.index_name == "mv_test_uq"


def test_postgres_backend_ordinary_refresh_failure_is_structured():
    from orm_loader.backends.materialized_view_errors import (
        MaterializationError,
        MaterializationOperation,
    )

    original = RuntimeError("boom")
    backend = PostgresBackend()
    session = _FakeSession(raise_on_execute=original)

    with pytest.raises(MaterializationError) as exc_info:
        backend.refresh_materialized_view(
            _conn(session), "mv_test", schema="reporting"
        )

    assert exc_info.value.__cause__ is original
    assert exc_info.value.failure.operation is MaterializationOperation.REFRESH


def test_postgres_backend_refresh_concurrently_without_declared_unique_index_raises_before_executing():
    from orm_loader.backends.materialized_view_errors import ConcurrentRefreshNotEligibleError

    backend = PostgresBackend()
    session = _FakeSession()

    with pytest.raises(ConcurrentRefreshNotEligibleError, match="no simple unique index"):
        backend.refresh_materialized_view(
            _conn(session), "mv_test", schema="reporting", concurrently=True
        )

    assert session.statements == []


def test_postgres_backend_refresh_concurrently_declared_but_database_rejects_it_translates_error():
    """PostgreSQL itself rejects CONCURRENTLY when the declared index doesn't
    genuinely exist yet (raising psycopg.errors.ObjectNotInPrerequisiteState).
    We don't pre-check the catalog ourselves; we attempt the refresh and
    translate that specific rejection."""
    import psycopg.errors

    from orm_loader.backends.materialized_view_errors import ConcurrentRefreshNotEligibleError
    from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex

    orig = psycopg.errors.ObjectNotInPrerequisiteState(
        'cannot refresh materialized view "reporting.mv_test" concurrently\n'
        "HINT:  Create a unique index with no WHERE clause on one or more "
        "columns of the materialized view."
    )
    backend = PostgresBackend()
    session = _FakeSession(
        raise_on_execute=OperationalError("REFRESH ...", {}, orig)
    )
    index = MaterializedViewIndex(name="mv_test_row_id_uq", columns=("row_id",), unique=True)

    with pytest.raises(ConcurrentRefreshNotEligibleError, match="cannot refresh") as exc_info:
        backend.refresh_materialized_view(
            _conn(session),
            "mv_test",
            schema="reporting",
            concurrently=True,
            declared_indexes=(index,),
        )

    cause = exc_info.value.failure.cause
    assert isinstance(cause, OperationalError)
    assert cause.orig is orig
    assert session.statements == ["REFRESH MATERIALIZED VIEW CONCURRENTLY reporting.mv_test;"]


def test_postgres_backend_refresh_concurrently_unrelated_operational_error_is_structured():
    """A different psycopg error (a lock timeout, a dropped connection, ...)
    is not a concurrent-refresh-eligibility problem and must not be
    misclassified as one."""
    import psycopg.errors

    from orm_loader.backends.materialized_view_errors import MaterializationError
    from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex

    orig = psycopg.errors.QueryCanceled("canceling statement due to statement timeout")
    backend = PostgresBackend()
    session = _FakeSession(
        raise_on_execute=OperationalError("REFRESH ...", {}, orig)
    )
    index = MaterializedViewIndex(name="mv_test_row_id_uq", columns=("row_id",), unique=True)

    with pytest.raises(MaterializationError) as exc_info:
        backend.refresh_materialized_view(
            _conn(session),
            "mv_test",
            schema="reporting",
            concurrently=True,
            declared_indexes=(index,),
        )

    cause = exc_info.value.__cause__
    assert isinstance(cause, OperationalError)
    assert cause.orig is orig
    assert exc_info.value.failure.cause is cause


def test_postgres_backend_refresh_concurrently_with_declared_index_emits_concurrently():
    backend = PostgresBackend()
    session = _FakeSession()
    from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex

    index = MaterializedViewIndex(name="mv_test_row_id_uq", columns=("row_id",), unique=True)

    backend.refresh_materialized_view(
        _conn(session),
        "mv_test",
        schema="reporting",
        concurrently=True,
        declared_indexes=(index,),
    )

    assert session.statements[-1] == "REFRESH MATERIALIZED VIEW CONCURRENTLY reporting.mv_test;"


def test_postgres_backend_materialized_view_lifecycle_is_schema_isolated_with_adversarial_identifiers(
    pg_db,
):
    """Real create/index/refresh(concurrently)/drop across two schemas
    sharing an adversarial (embedded-quote, embedded-space) view name,
    proving schema qualification actually isolates same-named views and
    that a genuinely-created eligible index really does permit
    CONCURRENTLY against the live database, not just against emitted SQL
    text."""
    from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex

    backend = PostgresBackend()
    conn = pg_db.connection
    left_schema, right_schema = 'mv "left" schema', 'mv "right" schema'
    name = 'shared "view" name'
    index = MaterializedViewIndex(name="shared_name_row_id_uq", columns=("row_id",), unique=True)
    selectable = sa.select(sa.literal(1).label("row_id"))

    for schema in (left_schema, right_schema):
        preparer = postgresql.dialect().identifier_preparer
        conn.execute(sa.text(f"CREATE SCHEMA {preparer.quote_identifier(schema)}"))
        backend.create_materialized_view(conn, name, selectable, schema=schema)
        backend.create_materialized_view_index(conn, name, index, schema=schema)

    backend.refresh_materialized_view(
        conn, name, schema=left_schema, concurrently=True, declared_indexes=(index,)
    )
    backend.drop_materialized_view(conn, name, schema=left_schema)

    # The left-schema view (and only it) is gone; the identically-named
    # right-schema view is untouched.
    assert (
        conn.execute(
            sa.text(
                "SELECT EXISTS (SELECT 1 FROM pg_matviews "
                "WHERE schemaname = :schema AND matviewname = :name)"
            ),
            {"schema": left_schema, "name": name},
        ).scalar()
        is False
    )
    assert (
        conn.execute(
            sa.text(
                "SELECT EXISTS (SELECT 1 FROM pg_matviews "
                "WHERE schemaname = :schema AND matviewname = :name)"
            ),
            {"schema": right_schema, "name": name},
        ).scalar()
        is True
    )


def test_postgres_backend_refresh_concurrently_raises_when_declared_index_was_never_created(pg_db):
    """The common real mistake: __mv_indexes__ declares a unique index but
    create_mv()'s index-creation step was never actually run (or the index
    was later dropped). PostgreSQL's own CONCURRENTLY prerequisite check
    catches it; we translate that rejection rather than silently attempting
    a blocking refresh or leaving a raw psycopg error to leak through."""
    from orm_loader.backends.materialized_view_errors import ConcurrentRefreshNotEligibleError
    from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex

    backend = PostgresBackend()
    conn = pg_db.connection
    conn.execute(sa.text("DROP MATERIALIZED VIEW IF EXISTS mv_missing_index_test"))
    selectable = sa.select(sa.literal(1).label("row_id"))
    backend.create_materialized_view(conn, "mv_missing_index_test", selectable)
    # Declared in Python, but create_materialized_view_index() was never called.
    index = MaterializedViewIndex(name="mv_missing_index_test_uq", columns=("row_id",), unique=True)

    with pytest.raises(ConcurrentRefreshNotEligibleError) as exc_info:
        backend.refresh_materialized_view(
            conn, "mv_missing_index_test", concurrently=True, declared_indexes=(index,)
        )

    assert "concurrently" in str(exc_info.value).lower()
    assert isinstance(exc_info.value.__cause__, OperationalError)


def test_postgres_backend_materialized_view_legacy_unqualified_path_still_round_trips(pg_db):
    """No schema override at all, matching how existing callers use these
    methods today: create/refresh/drop must still work end to end."""
    backend = PostgresBackend()
    conn = pg_db.connection
    selectable = sa.select(sa.literal(1).label("n"))

    backend.create_materialized_view(conn, "mv_legacy_test", selectable)
    backend.refresh_materialized_view(conn, "mv_legacy_test")
    assert conn.execute(sa.text("SELECT n FROM mv_legacy_test")).scalar() == 1

    backend.drop_materialized_view(conn, "mv_legacy_test")
    with pytest.raises(ProgrammingError):
        conn.execute(sa.text("SELECT n FROM mv_legacy_test"))


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
