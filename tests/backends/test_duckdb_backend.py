from __future__ import annotations

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Type, cast

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import CDMDatabaseConfig, ConnectionConfig, Resolver, StackConfig

from orm_loader.backends import Dialect, DuckDBBackend, staging_schema_claim
from orm_loader.backends.base import DeferredForeignKeyViolationError
from orm_loader.helpers.bootstrap import create_tables
from tests.models import (
    Base,
    ComputedColumnTable,
    CycleATable,
    CycleBTable,
    ReferencedParentTable,
    ReferencingChildTable,
    SelfReferentialTable,
    UnreferencedGrandparentTable,
    VocabReferencingTable,
    VocabSchemaTable,
)

if TYPE_CHECKING:
    from orm_loader.tables.typing import FileTableProtocol

_ComputedTableCls = cast("Type[FileTableProtocol]", ComputedColumnTable)
_CycleATableCls = cast("Type[FileTableProtocol]", CycleATable)
_CycleBTableCls = cast("Type[FileTableProtocol]", CycleBTable)
_ReferencedParentTableCls = cast("Type[FileTableProtocol]", ReferencedParentTable)
_SelfReferentialTableCls = cast("Type[FileTableProtocol]", SelfReferentialTable)
_VocabReferencingTableCls = cast("Type[FileTableProtocol]", VocabReferencingTable)
_VocabSchemaTableCls = cast("Type[FileTableProtocol]", VocabSchemaTable)
_TARGET_TABLE = ComputedColumnTable.__tablename__
_STAGING_TABLE = f"_staging_{_TARGET_TABLE}"


@pytest.fixture
def duckdb_backend(duckdb_db):
    """DuckDBBackend carrying duckdb_db's own resolved topology, needed by
    any FK-classification call now that resolved is required unconditionally.
    """
    return DuckDBBackend(resolved=duckdb_db.resolved)


@pytest.fixture
def duckdb_session_with_committed_schema(duckdb_db):
    """Same database duckdb_session provides, with the schema DDL actually
    committed, so a test can roll back its own later work mid-test without
    also discarding the schema.
    """
    conn = duckdb_db.connection
    Base.metadata.create_all(conn)
    conn.commit()
    return duckdb_db.session


@pytest.fixture
def duckdb_split_schema_session():
    """A real DuckDB file with PRIMARY in ``clinical`` and VOCAB in
    ``vocabulary``, so VocabReferencingTable's foreign key genuinely crosses
    a physical schema and gets omitted from its DDL.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        stack = StackConfig.for_session(
            connections={
                "db": ConnectionConfig(
                    dialect="duckdb", database_name=str(Path(tmp_dir) / "split.duckdb"), test_only=True
                )
            },
            databases={
                "cdm": CDMDatabaseConfig(
                    connection="db", cdm_schema="clinical", vocab_schema="vocabulary", results_schema="results"
                )
            },
        )
        resolved = Resolver(stack).resolve_database("cdm")
        primary_engine, _ = resolved.create_engines(schema_claims=[staging_schema_claim()])
        try:
            with primary_engine.connect() as conn:
                create_tables(
                    conn, [VocabReferencingTable.__table__, VocabSchemaTable.__table__], resolved=resolved
                )
                conn.commit()
                with so.Session(bind=conn) as session:
                    yield resolved, session
        finally:
            primary_engine.dispose()


@pytest.fixture
def duckdb_split_database_session():
    """PRIMARY and VOCAB on two separate DuckDB files/connections, so
    VocabReferencingTable's foreign key is classified "cross_database"
    rather than "cross_schema". No single query can join the two.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        stack = StackConfig.for_session(
            connections={
                "primary": ConnectionConfig(
                    dialect="duckdb", database_name=str(Path(tmp_dir) / "primary.duckdb"), test_only=True
                ),
                "vocab": ConnectionConfig(
                    dialect="duckdb", database_name=str(Path(tmp_dir) / "vocab.duckdb"), test_only=True
                ),
            },
            databases={"cdm": CDMDatabaseConfig(connection="primary", vocab_connection="vocab")},
        )
        resolved = Resolver(stack).resolve_database("cdm")
        primary_engine, vocab_engine = resolved.create_engines(schema_claims=[staging_schema_claim()])
        try:
            with vocab_engine.connect() as vocab_conn:
                create_tables(vocab_conn, [VocabSchemaTable.__table__], resolved=resolved)
                vocab_conn.commit()
            with primary_engine.connect() as conn:
                create_tables(conn, [VocabReferencingTable.__table__], resolved=resolved)
                conn.commit()
                with so.Session(bind=conn) as session:
                    yield resolved, session
        finally:
            primary_engine.dispose()
            vocab_engine.dispose()


@pytest.fixture
def duckdb_split_database_session_with_ephemeral_vocab():
    """Same split topology as duckdb_split_database_session, but VOCAB is
    an in-memory DuckDB database instead of a file, to exercise the
    ephemeral-connection guard in _validate_cross_database_fk.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        stack = StackConfig.for_session(
            connections={
                "primary": ConnectionConfig(
                    dialect="duckdb", database_name=str(Path(tmp_dir) / "primary.duckdb"), test_only=True
                ),
                "vocab": ConnectionConfig(dialect="duckdb", database_name=":memory:", test_only=True),
            },
            databases={"cdm": CDMDatabaseConfig(connection="primary", vocab_connection="vocab")},
        )
        resolved = Resolver(stack).resolve_database("cdm")
        primary_engine, _ = resolved.create_engines(schema_claims=[staging_schema_claim()])
        try:
            with primary_engine.connect() as conn:
                create_tables(conn, [VocabReferencingTable.__table__], resolved=resolved)
                conn.commit()
                with so.Session(bind=conn) as session:
                    yield resolved, session
        finally:
            primary_engine.dispose()


def test_duckdb_backend_identity_and_capabilities():
    backend = DuckDBBackend()

    assert backend.name == "duckdb"
    assert backend.dialect == Dialect.DUCKDB
    assert backend.supports_dialect(Dialect.DUCKDB) is True
    assert backend.capabilities.supports_unlogged_staging is False
    assert backend.capabilities.supports_fk_toggle is False
    assert backend.capabilities.supports_materialized_views is False
    assert backend.capabilities.supports_replace_with_kept_indexes is False
    assert backend.resolve_index_strategy("auto") == "drop_rebuild"


@pytest.mark.duckdb
@pytest.mark.db_dialect
@pytest.mark.parametrize("merge_batch_size", [0, -1])
@pytest.mark.parametrize("merge_strategy", ["replace", "upsert", "insert"])
def test_duckdb_backend_rejects_non_positive_merge_batch_size(merge_batch_size, merge_strategy, duckdb_session):
    backend = DuckDBBackend()
    args = [_ComputedTableCls, duckdb_session]
    if merge_strategy in {"replace", "upsert"}:
        args.append(["id"])
    merge = getattr(backend, f"merge_{merge_strategy}")

    with pytest.raises(ValueError, match="merge_batch_size must be a positive integer or None"):
        merge(*args, merge_batch_size=merge_batch_size)


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_create_staging_table(duckdb_session):
    backend = DuckDBBackend()

    backend.create_staging_table(_ComputedTableCls, duckdb_session)
    inspector = sa.inspect(duckdb_session.get_bind())
    assert inspector.has_table(_STAGING_TABLE) is True
    cols = inspector.get_columns(_STAGING_TABLE)
    assert [c["name"] for c in cols] == ["id", "name", "slug", "_rownum"]
    assert all(c["nullable"] is True for c in cols if c["name"] != "_rownum")


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_drop_staging_table(duckdb_session):
    backend = DuckDBBackend()

    backend.create_staging_table(_ComputedTableCls, duckdb_session)
    backend.drop_staging_table(_ComputedTableCls, duckdb_session)

    inspector = sa.inspect(duckdb_session.get_bind())
    assert inspector.has_table(_STAGING_TABLE) is False


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_drop_staging_table_also_drops_rownum_sequence(duckdb_session):
    """The staging table's _rownum column is backed by a real sequence, so
    dropping the staging table must drop that sequence too.
    """
    backend = DuckDBBackend()
    backend.create_staging_table(_ComputedTableCls, duckdb_session)

    sequences_while_present = duckdb_session.execute(
        sa.text("SELECT sequence_name FROM duckdb_sequences()")
    ).scalars().all()
    assert any(f"{_STAGING_TABLE}_rownum_seq" in name for name in sequences_while_present)

    backend.drop_staging_table(_ComputedTableCls, duckdb_session)

    sequences_after_drop = duckdb_session.execute(
        sa.text("SELECT sequence_name FROM duckdb_sequences()")
    ).scalars().all()
    assert not any(f"{_STAGING_TABLE}_rownum_seq" in name for name in sequences_after_drop)


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_rownum_populates_monotonically_on_insert(duckdb_session):
    backend = DuckDBBackend()
    backend.create_staging_table(_ComputedTableCls, duckdb_session)
    staging = _ComputedTableCls.get_staging_table(duckdb_session, staging_schema_tag=backend.staging_schema_tag)

    duckdb_session.execute(sa.insert(staging), [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])

    rownums = duckdb_session.execute(sa.select(staging.c._rownum).order_by(staging.c.id)).scalars().all()
    assert rownums == [1, 2]


def test_duckdb_backend_fk_toggle_methods_raise():
    backend = DuckDBBackend()

    with pytest.raises(NotImplementedError, match="no foreign-key-toggle mechanism"):
        backend.disable_fk_check(cast("so.Session", None))
    with pytest.raises(NotImplementedError, match="no foreign-key-toggle mechanism"):
        backend.enable_fk_check(cast("so.Session", None))
    with pytest.raises(NotImplementedError, match="no foreign-key-toggle mechanism"):
        backend.restore_fk_check(cast("so.Session", None), "ON")
    with pytest.raises(NotImplementedError, match="no state to normalize"):
        DuckDBBackend._normalize_fk_check_state("ON")


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_context_runs_without_toggling_fk(duckdb_session):
    """merge_context() must not require supports_fk_toggle: DuckDB has none."""
    backend = DuckDBBackend()
    backend.create_staging_table(_ComputedTableCls, duckdb_session)

    with backend.merge_context(_ComputedTableCls, duckdb_session):
        pass


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_materialized_view_methods_raise(duckdb_session):
    backend = DuckDBBackend()
    selectable = sa.select(sa.literal(1).label("n"))
    engine = duckdb_session.get_bind()

    with pytest.raises(NotImplementedError, match="does not support materialized views"):
        backend.create_materialized_view(engine, "mv_test", selectable)
    with pytest.raises(NotImplementedError, match="does not support materialized views"):
        backend.refresh_materialized_view(engine, "mv_test")
    with pytest.raises(NotImplementedError, match="does not support materialized views"):
        backend.drop_materialized_view(engine, "mv_test")


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_deferred_fk_constraints_finds_the_cycle(duckdb_session, duckdb_backend):
    a_fks = duckdb_backend.deferred_fk_constraints(_CycleATableCls)
    b_fks = duckdb_backend.deferred_fk_constraints(_CycleBTableCls)

    assert [c.name for c in a_fks[0].columns] == ["b_id"]
    assert [c.name for c in b_fks[0].columns] == ["a_id"]


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_validate_deferred_fks_passes_for_consistent_data(duckdb_session, duckdb_backend):
    duckdb_session.execute(sa.insert(CycleBTable), [{"id": 1, "a_id": None}])
    duckdb_session.execute(sa.insert(CycleATable), [{"id": 1, "b_id": 1}])

    duckdb_backend.validate_deferred_fks(_CycleATableCls, duckdb_session)
    duckdb_backend.validate_deferred_fks(_CycleBTableCls, duckdb_session)


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_validate_deferred_fks_catches_an_orphan(duckdb_session, duckdb_backend):
    """The cyclic FK was never created as a real constraint, so an orphaned
    reference inserts without error; only the anti-join check catches it.
    """
    duckdb_session.execute(sa.insert(CycleATable), [{"id": 1, "b_id": 999}])

    with pytest.raises(DeferredForeignKeyViolationError, match="cycle_a_table"):
        duckdb_backend.validate_deferred_fks(_CycleATableCls, duckdb_session)


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_replace_updates_in_place(duckdb_session):
    """merge_replace updates matching rows in place and leaves non-matching
    staging rows for the follow-up merge_insert() call.
    """
    backend = DuckDBBackend()
    backend.create_staging_table(_ComputedTableCls, duckdb_session)
    staging = _ComputedTableCls.get_staging_table(duckdb_session, staging_schema_tag=backend.staging_schema_tag)

    duckdb_session.execute(
        sa.insert(ComputedColumnTable),
        [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}],
    )
    duckdb_session.execute(
        sa.insert(staging), [{"id": 1, "name": "alpha-staged"}, {"id": 3, "name": "gamma"}]
    )

    backend.merge_replace(_ComputedTableCls, duckdb_session, ["id"])
    backend.merge_insert(_ComputedTableCls, duckdb_session)

    rows = {r.id: r.name for r in duckdb_session.execute(sa.select(ComputedColumnTable)).scalars().all()}
    assert rows == {1: "alpha-staged", 2: "beta", 3: "gamma"}


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_replace_paginated_updates_in_place(duckdb_session):
    backend = DuckDBBackend()
    backend.create_staging_table(_ComputedTableCls, duckdb_session)
    staging = _ComputedTableCls.get_staging_table(duckdb_session, staging_schema_tag=backend.staging_schema_tag)

    duckdb_session.execute(
        sa.insert(ComputedColumnTable), [{"id": i, "name": f"orig{i}"} for i in range(10)]
    )
    duckdb_session.execute(sa.insert(staging), [{"id": i, "name": f"staged{i}"} for i in range(10)])

    backend.merge_replace(_ComputedTableCls, duckdb_session, ["id"], merge_batch_size=3)
    backend.merge_insert(_ComputedTableCls, duckdb_session, merge_batch_size=3)

    rows = {r.id: r.name for r in duckdb_session.execute(sa.select(ComputedColumnTable)).scalars().all()}
    assert rows == {i: f"staged{i}" for i in range(10)}


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_replace_rejects_referenced_table_with_own_fk(duckdb_session, duckdb_backend):
    """merge_replace must reject a still-referenced row up front, before
    touching any data, rather than partway through a batch.
    """
    duckdb_backend.create_staging_table(_ReferencedParentTableCls, duckdb_session)
    staging = _ReferencedParentTableCls.get_staging_table(
        duckdb_session, staging_schema_tag=duckdb_backend.staging_schema_tag
    )

    duckdb_session.execute(sa.insert(UnreferencedGrandparentTable), [{"id": 1, "name": "g1"}])
    duckdb_session.execute(
        sa.insert(ReferencedParentTable), [{"id": 1, "grandparent_id": 1, "name": "p1"}]
    )
    duckdb_session.execute(sa.insert(ReferencingChildTable), [{"id": 1, "parent_id": 1}])
    duckdb_session.execute(sa.insert(staging), [{"id": 1, "grandparent_id": 1, "name": "p1-updated"}])

    with pytest.raises(ValueError, match="referenced_parent_table"):
        duckdb_backend.merge_replace(_ReferencedParentTableCls, duckdb_session, ["id"])


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_insert_batch_validation_prevents_partial_commit(
    duckdb_session_with_committed_schema, duckdb_backend,
):
    """merge_batch_size=1 commits each row separately. Row 3's own batch
    (the orphan) must be caught and never committed, while rows 1 and 2,
    already validated and committed in their own batches, survive.
    """
    session = duckdb_session_with_committed_schema
    duckdb_backend.create_staging_table(_CycleATableCls, session)
    staging = _CycleATableCls.get_staging_table(session, staging_schema_tag=duckdb_backend.staging_schema_tag)
    session.execute(sa.insert(staging), [{"id": 1, "b_id": None}, {"id": 2, "b_id": None}, {"id": 3, "b_id": 999}])

    with pytest.raises(DeferredForeignKeyViolationError, match="cycle_a_table"):
        duckdb_backend.merge_insert(_CycleATableCls, session, merge_batch_size=1)

    session.rollback()
    remaining = session.execute(sa.select(CycleATable.id).order_by(CycleATable.id)).scalars().all()
    assert remaining == [1, 2]


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_insert_loads_self_referential_rows_in_dependency_order(
    duckdb_session, duckdb_backend,
):
    """Staging rows arrive in reverse dependency order (each pointing at a
    row not yet committed); merge_insert must still load every row, in
    however many batches that takes, regardless of insertion order.
    """
    duckdb_backend.create_staging_table(_SelfReferentialTableCls, duckdb_session)
    staging = _SelfReferentialTableCls.get_staging_table(
        duckdb_session, staging_schema_tag=duckdb_backend.staging_schema_tag
    )
    duckdb_session.execute(
        sa.insert(staging),
        [{"id": 3, "parent_id": 2}, {"id": 2, "parent_id": 1}, {"id": 1, "parent_id": None}],
    )

    duckdb_backend.merge_insert(_SelfReferentialTableCls, duckdb_session, merge_batch_size=1)

    rows = {r.id: r.parent_id for r in duckdb_session.execute(sa.select(SelfReferentialTable)).scalars().all()}
    assert rows == {1: None, 2: 1, 3: 2}


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_upsert_loads_self_referential_rows_in_dependency_order(
    duckdb_session, duckdb_backend,
):
    duckdb_backend.create_staging_table(_SelfReferentialTableCls, duckdb_session)
    staging = _SelfReferentialTableCls.get_staging_table(
        duckdb_session, staging_schema_tag=duckdb_backend.staging_schema_tag
    )
    duckdb_session.execute(sa.insert(staging), [{"id": 2, "parent_id": 1}, {"id": 1, "parent_id": None}])

    duckdb_backend.merge_upsert(_SelfReferentialTableCls, duckdb_session, ["id"], merge_batch_size=1)

    rows = {r.id: r.parent_id for r in duckdb_session.execute(sa.select(SelfReferentialTable)).scalars().all()}
    assert rows == {1: None, 2: 1}


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_replace_updates_and_inserts_self_referential_rows_together(
    duckdb_session, duckdb_backend,
):
    """An existing row is re-pointed at one only this load adds. Processed
    as two sealed phases, the update would be validated before its target
    exists; merge_replace must do both together, in dependency order.
    """
    duckdb_session.execute(sa.insert(SelfReferentialTable), [{"id": 1, "parent_id": None}])
    duckdb_backend.create_staging_table(_SelfReferentialTableCls, duckdb_session)
    staging = _SelfReferentialTableCls.get_staging_table(
        duckdb_session, staging_schema_tag=duckdb_backend.staging_schema_tag
    )
    duckdb_session.execute(
        sa.insert(staging), [{"id": 1, "parent_id": 2}, {"id": 2, "parent_id": None}]
    )

    duckdb_backend.merge_replace(_SelfReferentialTableCls, duckdb_session, ["id"], merge_batch_size=1)

    rows = {r.id: r.parent_id for r in duckdb_session.execute(sa.select(SelfReferentialTable)).scalars().all()}
    assert rows == {1: 2, 2: None}
    assert duckdb_session.execute(sa.select(staging.c.id)).scalars().all() == []

    duckdb_backend.merge_insert(_SelfReferentialTableCls, duckdb_session)
    rows_after_insert = {
        r.id: r.parent_id for r in duckdb_session.execute(sa.select(SelfReferentialTable)).scalars().all()
    }
    assert rows_after_insert == rows


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_replace_catches_a_genuine_orphan(
    duckdb_session_with_committed_schema, duckdb_backend,
):
    """A replace batch re-pointing an existing row at a genuine orphan (not
    merely a not-yet-inserted new row) must still be caught, not mistaken
    for a forward reference that will resolve later. Rolling back must
    restore the original row.
    """
    session = duckdb_session_with_committed_schema
    session.execute(sa.insert(SelfReferentialTable), [{"id": 1, "parent_id": None}])
    duckdb_backend.create_staging_table(_SelfReferentialTableCls, session)
    staging = _SelfReferentialTableCls.get_staging_table(
        session, staging_schema_tag=duckdb_backend.staging_schema_tag
    )
    session.execute(sa.insert(staging), [{"id": 1, "parent_id": 999}])

    with pytest.raises(DeferredForeignKeyViolationError, match="self_referential_table"):
        duckdb_backend.merge_replace(_SelfReferentialTableCls, session, ["id"], merge_batch_size=1)

    session.rollback()
    assert session.execute(sa.select(SelfReferentialTable.parent_id)).scalars().all() == [None]
    assert session.execute(sa.select(staging.c.id)).scalars().all() == [1]


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_insert_catches_a_genuine_orphan_in_its_own_batch(
    duckdb_session_with_committed_schema, duckdb_backend,
):
    """A row whose parent never arrives is a genuine violation, caught in
    its own batch's commit. A row that became ready in an earlier batch
    already committed and survives rolling back the failed one.
    """
    session = duckdb_session_with_committed_schema
    duckdb_backend.create_staging_table(_SelfReferentialTableCls, session)
    staging = _SelfReferentialTableCls.get_staging_table(
        session, staging_schema_tag=duckdb_backend.staging_schema_tag
    )
    session.execute(sa.insert(staging), [{"id": 1, "parent_id": None}, {"id": 2, "parent_id": 999}])

    with pytest.raises(DeferredForeignKeyViolationError, match="self_referential_table"):
        duckdb_backend.merge_insert(_SelfReferentialTableCls, session, merge_batch_size=1)

    session.rollback()
    assert session.execute(sa.select(SelfReferentialTable.id)).scalars().all() == [1]
    assert session.execute(sa.select(staging.c.id)).scalars().all() == [2]


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_insert_unbatched_has_no_intermediate_commit(
    duckdb_session_with_committed_schema, duckdb_backend,
):
    """Without merge_batch_size, nothing may commit until the caller's own
    validation runs, even when a later row is a genuine orphan. Rolling
    back after that validation must leave nothing behind. Comparable to
    test_duckdb_backend_merge_insert_batch_validation_prevents_partial_commit,
    but with no batching requested at all.
    """
    session = duckdb_session_with_committed_schema
    duckdb_backend.create_staging_table(_SelfReferentialTableCls, session)
    staging = _SelfReferentialTableCls.get_staging_table(
        session, staging_schema_tag=duckdb_backend.staging_schema_tag
    )
    session.execute(sa.insert(staging), [{"id": 1, "parent_id": None}, {"id": 2, "parent_id": 999}])

    duckdb_backend.merge_insert(_SelfReferentialTableCls, session, merge_batch_size=None)
    with pytest.raises(DeferredForeignKeyViolationError, match="self_referential_table"):
        duckdb_backend.validate_deferred_fks(_SelfReferentialTableCls, session)

    session.rollback()
    assert session.execute(sa.select(SelfReferentialTable.id)).scalars().all() == []


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_insert_accepts_a_self_link(duckdb_session, duckdb_backend):
    """A row whose parent is itself is valid once it exists, not an orphan
    merely because the target was empty when it was considered.
    """
    duckdb_backend.create_staging_table(_SelfReferentialTableCls, duckdb_session)
    staging = _SelfReferentialTableCls.get_staging_table(
        duckdb_session, staging_schema_tag=duckdb_backend.staging_schema_tag
    )
    duckdb_session.execute(sa.insert(staging), [{"id": 1, "parent_id": 1}])

    duckdb_backend.merge_insert(_SelfReferentialTableCls, duckdb_session, merge_batch_size=1)
    duckdb_backend.validate_deferred_fks(_SelfReferentialTableCls, duckdb_session)

    rows = {r.id: r.parent_id for r in duckdb_session.execute(sa.select(SelfReferentialTable)).scalars().all()}
    assert rows == {1: 1}


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_insert_accepts_a_two_row_cycle(duckdb_session, duckdb_backend):
    """Two rows referencing each other are valid once both exist. Neither
    can ever become ready on its own, which is not a reason to reject them.
    """
    duckdb_backend.create_staging_table(_SelfReferentialTableCls, duckdb_session)
    staging = _SelfReferentialTableCls.get_staging_table(
        duckdb_session, staging_schema_tag=duckdb_backend.staging_schema_tag
    )
    duckdb_session.execute(sa.insert(staging), [{"id": 1, "parent_id": 2}, {"id": 2, "parent_id": 1}])

    duckdb_backend.merge_insert(_SelfReferentialTableCls, duckdb_session, merge_batch_size=1)
    duckdb_backend.validate_deferred_fks(_SelfReferentialTableCls, duckdb_session)

    rows = {r.id: r.parent_id for r in duckdb_session.execute(sa.select(SelfReferentialTable)).scalars().all()}
    assert rows == {1: 2, 2: 1}


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_upsert_ignores_a_duplicate_with_a_bad_reference(
    duckdb_session_with_committed_schema, duckdb_backend,
):
    """A staging row ON CONFLICT DO NOTHING will discard must not be
    judged as if it were going to be stored.
    """
    session = duckdb_session_with_committed_schema
    session.execute(sa.insert(SelfReferentialTable), [{"id": 1, "parent_id": None}])
    duckdb_backend.create_staging_table(_SelfReferentialTableCls, session)
    staging = _SelfReferentialTableCls.get_staging_table(
        session, staging_schema_tag=duckdb_backend.staging_schema_tag
    )
    session.execute(sa.insert(staging), [{"id": 1, "parent_id": 999}])

    duckdb_backend.merge_upsert(_SelfReferentialTableCls, session, ["id"], merge_batch_size=1)
    duckdb_backend.validate_deferred_fks(_SelfReferentialTableCls, session)

    rows = {r.id: r.parent_id for r in session.execute(sa.select(SelfReferentialTable)).scalars().all()}
    assert rows == {1: None}


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_insert_respects_validate_deferred_fks_false(
    duckdb_session_with_committed_schema, duckdb_backend,
):
    """Opting out of application validation must not be overridden by the
    self-referential batching path.
    """
    session = duckdb_session_with_committed_schema
    duckdb_backend.create_staging_table(_SelfReferentialTableCls, session)
    staging = _SelfReferentialTableCls.get_staging_table(
        session, staging_schema_tag=duckdb_backend.staging_schema_tag
    )
    session.execute(sa.insert(staging), [{"id": 1, "parent_id": 999}])

    duckdb_backend.merge_insert(
        _SelfReferentialTableCls, session, merge_batch_size=1, validate_deferred_fks=False
    )

    rows = {r.id: r.parent_id for r in session.execute(sa.select(SelfReferentialTable)).scalars().all()}
    assert rows == {1: 999}


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_validate_deferred_fks_catches_a_cross_database_orphan(duckdb_split_database_session):
    """A cross-database FK is genuinely checked, via a second connection,
    not skipped. An orphan on the other database must still be caught.
    """
    resolved, session = duckdb_split_database_session
    backend = DuckDBBackend(resolved=resolved)
    session.execute(sa.insert(VocabReferencingTable), [{"id": 1, "vocab_id": 999}])

    with pytest.raises(DeferredForeignKeyViolationError, match="vocab_referencing_table"):
        backend.validate_deferred_fks(_VocabReferencingTableCls, session)


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_validate_deferred_fks_passes_a_real_cross_database_reference(
    duckdb_split_database_session,
):
    """A cross-database FK pointing at a row that genuinely exists on the
    other connection must pass, proving the check is real rather than a
    pass-through.
    """
    resolved, session = duckdb_split_database_session
    backend = DuckDBBackend(resolved=resolved)

    primary_engine, vocab_engine = resolved.create_engines(register_claims=False)
    try:
        with vocab_engine.connect() as vocab_conn:
            vocab_conn.execute(sa.insert(VocabSchemaTable), [{"id": 1, "name": "v1"}])
            vocab_conn.commit()
    finally:
        primary_engine.dispose()
        if vocab_engine is not primary_engine:
            vocab_engine.dispose()

    session.execute(sa.insert(VocabReferencingTable), [{"id": 1, "vocab_id": 1}])

    backend.validate_deferred_fks(_VocabReferencingTableCls, session)


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_validate_deferred_fks_refuses_an_ephemeral_remote_connection(
    duckdb_split_database_session_with_ephemeral_vocab,
):
    """A cross-database FK into an in-memory VOCAB must raise clearly
    instead of rebuilding a disconnected, empty engine and reporting
    every reference as missing.
    """
    resolved, session = duckdb_split_database_session_with_ephemeral_vocab
    backend = DuckDBBackend(resolved=resolved)
    session.execute(sa.insert(VocabReferencingTable), [{"id": 1, "vocab_id": 1}])

    with pytest.raises(ValueError, match="ephemeral"):
        backend.validate_deferred_fks(_VocabReferencingTableCls, session)


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_insert_batch_validation_is_a_no_op_without_deferred_fks(duckdb_session):
    """A table with no deferred/unenforced FK must not pay for a validation
    query on every batch commit.
    """
    backend = DuckDBBackend()
    backend.create_staging_table(_ComputedTableCls, duckdb_session)
    staging = _ComputedTableCls.get_staging_table(duckdb_session, staging_schema_tag=backend.staging_schema_tag)
    duckdb_session.execute(sa.insert(staging), [{"id": i, "name": f"n{i}"} for i in range(5)])

    backend.merge_insert(_ComputedTableCls, duckdb_session, merge_batch_size=1)

    rows = duckdb_session.execute(sa.select(ComputedColumnTable.id)).scalars().all()
    assert sorted(rows) == list(range(5))


def test_duckdb_backend_deferred_fk_constraints_without_resolved_raises_on_cross_schema_fk():
    """`resolved` is required unconditionally, not just when a cross-schema
    FK is actually present: a cheaper "cyclic-only" fallback could still
    misclassify an unrelated cross-schema FK elsewhere in the metadata.
    """
    backend = DuckDBBackend()

    with pytest.raises(ValueError, match="resolved"):
        backend.deferred_fk_constraints(_VocabReferencingTableCls)


def test_duckdb_backend_deferred_fk_constraints_without_resolved_raises_even_for_a_cyclic_fk():
    """Same requirement for a purely cyclic FK that never crosses a schema
    tag, since the method has no cheap way to tell that in advance.
    """
    backend = DuckDBBackend()

    with pytest.raises(ValueError, match="resolved"):
        backend.deferred_fk_constraints(_CycleATableCls)


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_deferred_fk_constraints_classifies_cross_schema_fk_with_resolved(
    duckdb_split_schema_session,
):
    """With `resolved`, the omitted cross-schema key is classified the same
    as a cyclic one: unenforced.
    """
    resolved, _session = duckdb_split_schema_session
    backend = DuckDBBackend(resolved=resolved)

    fks = backend.deferred_fk_constraints(_VocabReferencingTableCls)

    assert [c.name for c in fks[0].columns] == ["vocab_id"]


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_validate_deferred_fks_catches_a_cross_schema_orphan(duckdb_split_schema_session):
    """The cross-schema key was never created, so an orphaned reference
    inserts without error; only the anti-join check catches it.
    """
    resolved, session = duckdb_split_schema_session
    backend = DuckDBBackend(resolved=resolved)
    session.execute(sa.insert(VocabReferencingTable), [{"id": 1, "vocab_id": 999}])

    with pytest.raises(DeferredForeignKeyViolationError, match="vocab_referencing_table"):
        backend.validate_deferred_fks(_VocabReferencingTableCls, session)


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_tables_referencing_excludes_an_omitted_cross_schema_fk(duckdb_split_schema_session):
    """VocabReferencingTable's foreign key into VocabSchemaTable was never
    created, so check_replace_feasible() must not treat it as a real
    referencing table.
    """
    resolved, session = duckdb_split_schema_session
    backend = DuckDBBackend(resolved=resolved)
    session.execute(sa.insert(VocabSchemaTable), [{"id": 1, "name": "v1"}])
    session.execute(sa.insert(VocabReferencingTable), [{"id": 1, "vocab_id": 1}])

    assert backend._tables_referencing(VocabSchemaTable.__table__) == set()
    assert backend._is_referenced_by_existing_rows(VocabSchemaTable.__table__, session) is False
