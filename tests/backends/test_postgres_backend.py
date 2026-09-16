from __future__ import annotations

import sqlalchemy.event as sae
from typing import TYPE_CHECKING, Type, cast

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine

from oa_configurator import SCHEMA_TRANSLATE_MAP_KEY, Role
from oa_configurator.testing import isolated_test_schema
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
        schema_translate_map: dict[str, str | None] | None = None,
    ) -> None:
        self.statements: list[str] = []
        self.scalar_result = scalar_result
        self.raise_on_execute = raise_on_execute
        self.commits = 0
        self._schema_translate_map = schema_translate_map

    def get_execution_options(self) -> dict:
        """Minimal support for oa_configurator.schema_of(), which every
        materialized-view backend method resolves its target schema through."""
        if self._schema_translate_map is None:
            return {}
        return {SCHEMA_TRANSLATE_MAP_KEY: self._schema_translate_map}

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


def test_postgres_backend_materialized_view_respects_role(pg_db) -> None:
    """create_materialized_view()/refresh_materialized_view() used to always
    resolve schema=None -> schema_of(conn) with no role, which defaults to
    Role.PRIMARY regardless of what role the view was actually built over.
    A view over vocab-role tables must land in the vocab schema, not
    wherever primary happens to be."""
    backend = PostgresBackend()
    selectable = sa.select(sa.literal(1).label("n"))
    engine = pg_db.connection.engine

    with isolated_test_schema(engine, prefix="mv_primary") as primary_schema, \
         isolated_test_schema(engine, prefix="mv_vocab") as vocab_schema:
        scoped = engine.execution_options(
            schema_translate_map={Role.PRIMARY.value: primary_schema, "vocab": vocab_schema}
        )
        with scoped.begin() as conn:
            backend.create_materialized_view(conn, "mv_role_test", selectable, role=Role.VOCAB)
            backend.refresh_materialized_view(conn, "mv_role_test", role=Role.VOCAB)

        with engine.connect() as conn:
            assert sa.inspect(conn).has_table("mv_role_test", schema=vocab_schema)
            assert not sa.inspect(conn).has_table("mv_role_test", schema=primary_schema)
            assert conn.execute(
                sa.text(f'SELECT n FROM "{vocab_schema}".mv_role_test')
            ).scalar() == 1


def test_postgres_backend_materialized_view_methods_emit_expected_sql():
    backend = PostgresBackend()
    session = _FakeSession()
    selectable = sa.select(sa.literal(1).label("n"))

    backend.create_materialized_view(_sess(session), "mv_test", selectable)
    backend.refresh_materialized_view(_sess(session), "mv_test")

    assert any('CREATE MATERIALIZED VIEW IF NOT EXISTS mv_test as SELECT' in sql for sql in session.statements)
    assert any('REFRESH MATERIALIZED VIEW mv_test;' == sql for sql in session.statements)


def test_postgres_backend_rejects_mismatched_dialect_bind():
    from sqlalchemy.dialects import sqlite

    backend = PostgresBackend()
    session = _FakeSession()
    session.dialect = sqlite.dialect()
    selectable = sa.select(sa.literal(1).label("n"))

    with pytest.raises(TypeError, match="received a 'sqlite' connection; expected 'postgresql'"):
        backend.create_materialized_view(_sess(session), "mv_test", selectable)

    assert session.statements == []


def test_postgres_backend_rejects_dialect_that_drifted_after_resolve_backend():
    """A bind resolved to PostgresBackend, then a differently-dialected bind
    handed to one of its methods, must not run Postgres-only DDL against it."""
    from orm_loader.backends.resolve import resolve_backend
    from sqlalchemy.dialects import sqlite

    postgres_session = _FakeSession()
    backend = resolve_backend(_sess(postgres_session))
    assert isinstance(backend, PostgresBackend)

    sqlite_session = _FakeSession()
    sqlite_session.dialect = sqlite.dialect()
    selectable = sa.select(sa.literal(1).label("n"))

    with pytest.raises(TypeError, match="received a 'sqlite' connection; expected 'postgresql'"):
        backend.create_materialized_view(_sess(sqlite_session), "mv_test", selectable)


def test_postgres_backend_quotes_unqualified_materialized_view_name():
    from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex

    backend = PostgresBackend()
    session = _FakeSession()
    selectable = sa.select(sa.literal(1).label("n"))

    backend.create_materialized_view(_sess(session), 'mv name', selectable)
    backend.create_materialized_view_index(
        _sess(session), 'mv name', MaterializedViewIndex(name="mv_name_idx", columns=("n",))
    )

    assert any('CREATE MATERIALIZED VIEW IF NOT EXISTS "mv name" as SELECT' in sql for sql in session.statements)
    assert any('ON "mv name" ("n")' in sql for sql in session.statements)


def test_postgres_backend_create_mv_quotes_name_for_legacy_search_path_resolution():
    backend = PostgresBackend()
    session = _FakeSession()
    backend.create_materialized_view(
        _sess(session), "MixedCaseMv", sa.select(sa.literal(1).label("n"))
    )

    assert any(
        'CREATE MATERIALIZED VIEW IF NOT EXISTS "MixedCaseMv" as SELECT' in sql
        for sql in session.statements
    )


def test_postgres_backend_create_materialized_view_index_emits_expected_sql():
    from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex

    backend = PostgresBackend()
    session = _FakeSession(schema_translate_map={Role.PRIMARY.value: "reporting"})
    index = MaterializedViewIndex(name="mv_test_row_id_uq", columns=("row_id",), unique=True)

    backend.create_materialized_view_index(_sess(session), "mv_test", index)

    assert session.statements == [
        'CREATE UNIQUE INDEX IF NOT EXISTS "mv_test_row_id_uq" ON reporting.mv_test ("row_id")'
    ]


def test_postgres_backend_create_materialized_view_index_failure_mentions_index_name():
    from orm_loader.mappers.materialised_view_errors import MaterializationError
    from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex

    original = RuntimeError("boom")
    backend = PostgresBackend()
    session = _FakeSession(raise_on_execute=original)
    index = MaterializedViewIndex(name="mv_test_row_id_uq", columns=("row_id",))

    with pytest.raises(MaterializationError, match="mv_test_row_id_uq"):
        backend.create_materialized_view_index(_sess(session), "mv_test", index)


def test_postgres_backend_drop_materialized_view_default_args():
    backend = PostgresBackend()
    session = _FakeSession(schema_translate_map={Role.PRIMARY.value: "reporting"})

    backend.drop_materialized_view(_sess(session), "mv_test")

    assert session.statements == ['DROP MATERIALIZED VIEW IF EXISTS reporting.mv_test']


def test_postgres_backend_drop_materialized_view_cascade_and_if_exists_false():
    backend = PostgresBackend()
    session = _FakeSession(schema_translate_map={Role.PRIMARY.value: "reporting"})

    backend.drop_materialized_view(_sess(session), "mv_test", if_exists=False, cascade=True)

    assert session.statements == ['DROP MATERIALIZED VIEW reporting.mv_test CASCADE']


def test_postgres_backend_drop_materialized_view_failure_preserves_cause():
    from orm_loader.mappers.materialised_view_errors import MaterializationError, MaterializationOperation

    original = RuntimeError("boom")
    backend = PostgresBackend()
    session = _FakeSession(
        raise_on_execute=original, schema_translate_map={Role.PRIMARY.value: "reporting"}
    )

    with pytest.raises(MaterializationError) as exc_info:
        backend.drop_materialized_view(_sess(session), "mv_test")

    assert exc_info.value.__cause__ is original
    assert exc_info.value.failure.cause is original
    assert exc_info.value.failure.operation is MaterializationOperation.DROP


def test_postgres_backend_create_materialized_view_failure_preserves_cause():
    from orm_loader.mappers.materialised_view_errors import MaterializationError, MaterializationOperation

    original = RuntimeError("boom")
    backend = PostgresBackend()
    session = _FakeSession(raise_on_execute=original)

    with pytest.raises(MaterializationError) as exc_info:
        backend.create_materialized_view(
            _sess(session), "mv_test", sa.select(sa.literal(1).label("n"))
        )

    assert exc_info.value.__cause__ is original
    assert exc_info.value.failure.cause is original
    assert exc_info.value.failure.operation is MaterializationOperation.CREATE


def test_postgres_backend_refresh_concurrently_without_declared_unique_index_raises_before_executing():
    from orm_loader.mappers.materialised_view_errors import ConcurrentRefreshNotEligibleError

    backend = PostgresBackend()
    session = _FakeSession(schema_translate_map={Role.PRIMARY.value: "reporting"})

    with pytest.raises(ConcurrentRefreshNotEligibleError, match="no simple unique index"):
        backend.refresh_materialized_view(_sess(session), "mv_test", concurrently=True)

    assert session.statements == []


def test_postgres_backend_refresh_concurrently_declared_but_database_rejects_it_translates_error():
    psycopg = pytest.importorskip("psycopg")

    from orm_loader.mappers.materialised_view_errors import ConcurrentRefreshNotEligibleError
    from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex

    orig = psycopg.errors.ObjectNotInPrerequisiteState(
        'cannot refresh materialized view "reporting.mv_test" concurrently\n'
        "HINT:  Create a unique index with no WHERE clause on one or more "
        "columns of the materialized view."
    )
    backend = PostgresBackend()
    session = _FakeSession(
        raise_on_execute=sa.exc.OperationalError("REFRESH ...", {}, orig),
        schema_translate_map={Role.PRIMARY.value: "reporting"},
    )
    index = MaterializedViewIndex(name="mv_test_row_id_uq", columns=("row_id",), unique=True)

    with pytest.raises(ConcurrentRefreshNotEligibleError, match="cannot refresh") as exc_info:
        backend.refresh_materialized_view(
            _sess(session),
            "mv_test",
            concurrently=True,
            declared_indexes=(index,),
        )

    assert exc_info.value.failure.cause.orig is orig
    assert session.statements == [
        'REFRESH MATERIALIZED VIEW CONCURRENTLY reporting.mv_test;'
    ]


def test_postgres_backend_refresh_concurrently_unrelated_operational_error_propagates_unchanged():
    psycopg = pytest.importorskip("psycopg")

    from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex

    orig = psycopg.errors.QueryCanceled("canceling statement due to statement timeout")
    backend = PostgresBackend()
    session = _FakeSession(
        raise_on_execute=sa.exc.OperationalError("REFRESH ...", {}, orig),
        schema_translate_map={Role.PRIMARY.value: "reporting"},
    )
    index = MaterializedViewIndex(name="mv_test_row_id_uq", columns=("row_id",), unique=True)

    with pytest.raises(sa.exc.OperationalError) as exc_info:
        backend.refresh_materialized_view(
            _sess(session),
            "mv_test",
            concurrently=True,
            declared_indexes=(index,),
        )

    assert exc_info.value.orig is orig


def test_postgres_backend_refresh_concurrently_with_declared_index_emits_concurrently():
    from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex

    backend = PostgresBackend()
    session = _FakeSession(schema_translate_map={Role.PRIMARY.value: "reporting"})
    index = MaterializedViewIndex(name="mv_test_row_id_uq", columns=("row_id",), unique=True)

    backend.refresh_materialized_view(
        _sess(session),
        "mv_test",
        concurrently=True,
        declared_indexes=(index,),
    )

    assert session.statements[-1] == (
        'REFRESH MATERIALIZED VIEW CONCURRENTLY reporting.mv_test;'
    )


def test_postgres_backend_materialized_view_lifecycle_is_schema_isolated_with_adversarial_identifiers(
    pg_db,
):
    """Two schemas, each addressed via its own scoped connection (role-based
    resolution ties the schema to the connection, not to a per-call
    override), must never bleed into each other even with adversarial,
    quote-laden identifiers."""
    from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex

    backend = PostgresBackend()
    left_schema, right_schema = 'mv "left" schema', 'mv "right" schema'
    name = 'shared "view" name'
    index = MaterializedViewIndex(name="shared_name_row_id_uq", columns=("row_id",), unique=True)
    selectable = sa.select(sa.literal(1).label("row_id"))
    engine = pg_db.connection.engine
    preparer = postgresql.dialect().identifier_preparer

    try:
        with engine.begin() as setup_conn:
            for schema in (left_schema, right_schema):
                setup_conn.execute(sa.text(f"CREATE SCHEMA {preparer.quote_identifier(schema)}"))

        left = engine.execution_options(schema_translate_map={Role.PRIMARY.value: left_schema})
        right = engine.execution_options(schema_translate_map={Role.PRIMARY.value: right_schema})

        for scoped in (left, right):
            with scoped.begin() as conn:
                backend.create_materialized_view(conn, name, selectable)
                backend.create_materialized_view_index(conn, name, index)

        with left.begin() as conn:
            backend.refresh_materialized_view(conn, name, concurrently=True, declared_indexes=(index,))
            backend.drop_materialized_view(conn, name)

        with engine.connect() as conn:
            assert conn.execute(
                sa.text(
                    "SELECT EXISTS (SELECT 1 FROM pg_matviews "
                    "WHERE schemaname = :schema AND matviewname = :name)"
                ),
                {"schema": left_schema, "name": name},
            ).scalar() is False
            assert conn.execute(
                sa.text(
                    "SELECT EXISTS (SELECT 1 FROM pg_matviews "
                    "WHERE schemaname = :schema AND matviewname = :name)"
                ),
                {"schema": right_schema, "name": name},
            ).scalar() is True
    finally:
        with engine.begin() as cleanup_conn:
            for schema in (left_schema, right_schema):
                cleanup_conn.execute(
                    sa.text(f"DROP SCHEMA IF EXISTS {preparer.quote_identifier(schema)} CASCADE")
                )


def test_postgres_backend_refresh_concurrently_raises_when_declared_index_was_never_created(pg_db):
    from orm_loader.mappers.materialised_view_errors import ConcurrentRefreshNotEligibleError
    from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex

    backend = PostgresBackend()
    index = MaterializedViewIndex(name="mv_missing_index_test_uq", columns=("row_id",), unique=True)
    conn = pg_db.connection

    backend.create_materialized_view(
        conn, "mv_missing_index_test", sa.select(sa.literal(1).label("row_id"))
    )

    with pytest.raises(ConcurrentRefreshNotEligibleError) as exc_info:
        backend.refresh_materialized_view(
            conn,
            "mv_missing_index_test",
            concurrently=True,
            declared_indexes=(index,),
        )

    assert "concurrently" in str(exc_info.value).lower()
    assert isinstance(exc_info.value.__cause__, sa.exc.OperationalError)


def test_postgres_backend_materialized_view_legacy_unqualified_path_still_round_trips(pg_db):
    backend = PostgresBackend()
    conn = pg_db.connection

    backend.create_materialized_view(
        conn, "mv_legacy_test", sa.select(sa.literal(1).label("n"))
    )
    backend.refresh_materialized_view(conn, "mv_legacy_test")
    assert conn.execute(sa.text("SELECT n FROM mv_legacy_test")).scalar() == 1

    backend.drop_materialized_view(conn, "mv_legacy_test")
    with pytest.raises(sa.exc.ProgrammingError):
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

        def get_isolation_level(self):
            return "READ COMMITTED"

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

        def execute(self, statement):
            sql = str(statement.compile(dialect=postgresql.dialect()))
            statements.append(sql)
            return _Result()

    class _Engine(Engine):
        def __init__(self) -> None:
            # only exists for autocommit_connection() to route it into its real Engine branch
            pass

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
