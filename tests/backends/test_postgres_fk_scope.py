"""Foreign-key relaxation follows transactions, without leaking into the pool."""

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import Role
from sqlalchemy.exc import IntegrityError

from orm_loader.backends import PostgresBackend

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize("end_transaction", ["commit", "rollback"])
def test_relaxation_follows_connection_swaps_without_affecting_other_sessions(
    pooled_pg_engine,
    end_transaction,
):
    backend = PostgresBackend()
    with so.Session(pooled_pg_engine) as session:
        with backend.relaxed_foreign_keys(session):
            first_pid = session.scalar(sa.text("SELECT pg_backend_pid()"))
            assert session.scalar(sa.text("SHOW session_replication_role")) == "replica"
            getattr(session, end_transaction)()
            second_pid = session.scalar(sa.text("SELECT pg_backend_pid()"))
            assert second_pid != first_pid
            assert session.scalar(sa.text("SHOW session_replication_role")) == "replica"
            with so.Session(pooled_pg_engine) as unrelated:
                assert unrelated.scalar(sa.text("SELECT pg_backend_pid()")) == first_pid
                assert unrelated.scalar(sa.text("SHOW session_replication_role")) == "origin"
        # Restore the still-open transaction immediately on context exit.
        assert session.scalar(sa.text("SHOW session_replication_role")) == "origin"
        session.commit()
        # A later transaction must not re-enable relaxation.
        assert session.scalar(sa.text("SHOW session_replication_role")) == "origin"
    with pooled_pg_engine.connect() as first, pooled_pg_engine.connect() as second:
        assert first.scalar(sa.text("SHOW session_replication_role")) == "origin"
        assert second.scalar(sa.text("SHOW session_replication_role")) == "origin"


def test_failed_scope_rolls_back_and_removes_transaction_listener(pooled_pg_engine):
    with so.Session(pooled_pg_engine) as session:
        with (
            pytest.raises(RuntimeError, match="stop the load"),
            PostgresBackend().relaxed_foreign_keys(session),
        ):
            session.execute(sa.text("SELECT 1"))
            session.commit()
            assert session.scalar(sa.text("SHOW session_replication_role")) == "replica"
            raise RuntimeError("stop the load")
        assert session.scalar(sa.text("SHOW session_replication_role")) == "origin"
    with pooled_pg_engine.connect() as first, pooled_pg_engine.connect() as second:
        assert first.scalar(sa.text("SHOW session_replication_role")) == "origin"
        assert second.scalar(sa.text("SHOW session_replication_role")) == "origin"


def test_nested_scopes_restore_the_existing_transaction_role(pooled_pg_engine):
    backend = PostgresBackend()
    with so.Session(pooled_pg_engine) as session:
        session.execute(sa.text("SET LOCAL session_replication_role = 'local'"))
        with backend.relaxed_foreign_keys(session):
            assert session.scalar(sa.text("SHOW session_replication_role")) == "replica"
            with backend.relaxed_foreign_keys(session):
                assert session.scalar(sa.text("SHOW session_replication_role")) == "replica"
            assert session.scalar(sa.text("SHOW session_replication_role")) == "replica"
        assert session.scalar(sa.text("SHOW session_replication_role")) == "local"
        session.rollback()
        assert session.scalar(sa.text("SHOW session_replication_role")) == "origin"


def test_relaxation_is_limited_to_the_mapper_bind(pg_db, pooled_pg_engine):
    from tests.models import EnumTable, SimpleTable

    other_engine = pg_db.committing_engine
    with (
        so.Session(binds={SimpleTable: pooled_pg_engine, EnumTable: other_engine}) as session,
        PostgresBackend().relaxed_foreign_keys(session, mapper=SimpleTable),
    ):
        for _ in range(2):
            target = session.connection(bind_arguments={"mapper": SimpleTable})
            unrelated = session.connection(bind_arguments={"mapper": EnumTable})
            assert target.scalar(sa.text("SHOW session_replication_role")) == "replica"
            assert unrelated.scalar(sa.text("SHOW session_replication_role")) == "origin"
            session.commit()


def test_scoped_session_relaxation_does_not_affect_another_thread(pooled_pg_engine):
    from concurrent.futures import ThreadPoolExecutor

    registry = so.scoped_session(so.sessionmaker(bind=pooled_pg_engine))

    def other_thread_role():
        try:
            return registry.scalar(sa.text("SHOW session_replication_role"))
        finally:
            registry.remove()

    try:
        with ThreadPoolExecutor(max_workers=1) as worker:
            with PostgresBackend().relaxed_foreign_keys(registry):
                assert registry.scalar(sa.text("SHOW session_replication_role")) == "replica"
                other_role = worker.submit(other_thread_role).result(timeout=10)
            restored_role = registry.scalar(sa.text("SHOW session_replication_role"))
            assert (other_role, restored_role) == ("origin", "origin")
        registry.commit()
        assert registry.scalar(sa.text("SHOW session_replication_role")) == "origin"
    finally:
        registry.remove()


class _ScopeBase(so.DeclarativeBase):
    metadata = sa.MetaData(schema=Role.PRIMARY.value)


class _ScopeParent(_ScopeBase):
    __tablename__ = "_fk_scope_parent"
    id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=False)


class _ScopeChild(_ScopeBase):
    __tablename__ = "_fk_scope_child"
    id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=False)
    parent_id: so.Mapped[int] = so.mapped_column(
        sa.ForeignKey(f"{Role.PRIMARY.value}._fk_scope_parent.id")
    )


@pytest.mark.parametrize("flush_inside", [False, True])
def test_orm_inserts_must_be_flushed_inside_relaxation(pooled_pg_engine, flush_inside):
    _ScopeBase.metadata.create_all(pooled_pg_engine)
    with so.Session(pooled_pg_engine) as session:
        with PostgresBackend().relaxed_foreign_keys(session):
            session.add_all([_ScopeChild(id=1, parent_id=999)])
            if flush_inside:
                session.flush()
        if flush_inside:
            session.commit()
        else:
            with pytest.raises(IntegrityError):
                session.commit()
    with so.Session(pooled_pg_engine) as verify:
        assert verify.scalar(sa.select(sa.func.count()).select_from(_ScopeChild)) == int(
            flush_inside
        )
