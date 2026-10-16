from __future__ import annotations

from typing import TYPE_CHECKING, Type, cast

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so

from orm_loader.backends import Dialect, DuckDBBackend
from orm_loader.backends.base import DeferredForeignKeyViolationError
from tests.models import (
    ComputedColumnTable,
    CycleATable,
    CycleBTable,
    ReferencedParentTable,
    ReferencingChildTable,
    UnreferencedGrandparentTable,
)

if TYPE_CHECKING:
    from orm_loader.tables.typing import CSVTableProtocol

_ComputedTableCls = cast("Type[CSVTableProtocol]", ComputedColumnTable)
_CycleATableCls = cast("Type[CSVTableProtocol]", CycleATable)
_CycleBTableCls = cast("Type[CSVTableProtocol]", CycleBTable)
_ReferencedParentTableCls = cast("Type[CSVTableProtocol]", ReferencedParentTable)
_TARGET_TABLE = ComputedColumnTable.__tablename__
_STAGING_TABLE = f"_staging_{_TARGET_TABLE}"


def test_duckdb_backend_identity_and_capabilities():
    backend = DuckDBBackend()

    assert backend.name == "duckdb"
    assert backend.dialect == Dialect.DUCKDB
    assert backend.supports_dialect(Dialect.DUCKDB) is True
    assert backend.capabilities.supports_fast_load is False
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
    """The staging table's _rownum column is backed by a real sequence, so dropping 
    the staging table must drop that sequence too.
    
    Notes
    -----
    DuckDB can't ALTER TABLE ADD a GENERATED ALWAYS AS IDENTITY column after creation
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
def test_duckdb_backend_deferred_fk_constraints_finds_the_cycle(duckdb_session):
    backend = DuckDBBackend()

    a_fks = backend.deferred_fk_constraints(_CycleATableCls)
    b_fks = backend.deferred_fk_constraints(_CycleBTableCls)

    assert [c.name for c in a_fks[0].columns] == ["b_id"]
    assert [c.name for c in b_fks[0].columns] == ["a_id"]


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_validate_deferred_fks_passes_for_consistent_data(duckdb_session):
    backend = DuckDBBackend()
    duckdb_session.execute(sa.insert(CycleBTable), [{"id": 1, "a_id": None}])
    duckdb_session.execute(sa.insert(CycleATable), [{"id": 1, "b_id": 1}])

    backend.validate_deferred_fks(_CycleATableCls, duckdb_session)
    backend.validate_deferred_fks(_CycleBTableCls, duckdb_session)


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_validate_deferred_fks_catches_an_orphan(duckdb_session):
    """DuckDB's engine never created this FK as a real constraint (it's part
    of a cycle, so oa-configurator's shim no-opped the ALTER TABLE ADD
    CONSTRAINT), so an orphaned reference inserts without error; the
    anti-join check is the only thing that can catch it."""
    backend = DuckDBBackend()
    duckdb_session.execute(sa.insert(CycleATable), [{"id": 1, "b_id": 999}])

    with pytest.raises(DeferredForeignKeyViolationError, match="cycle_a_table"):
        backend.validate_deferred_fks(_CycleATableCls, duckdb_session)


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_duckdb_backend_merge_replace_updates_in_place(duckdb_session):
    """merge_replace cannot delete a still-referenced row (no FK-toggle escape
    hatch), so it updates matching rows in place and leaves non-matching staging
    rows for the follow-up merge_insert() call, matching merge_from_staging()'s
    real two-step orchestration rather than merge_replace alone.
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
def test_duckdb_backend_merge_replace_rejects_referenced_table_with_own_fk(duckdb_session):
    """DuckDB rejects a SET on any FK-constrained column of a row that's
    still referenced elsewhere, regardless of index_strategy. merge_replace
    must fail up front, before touching any data, rather than partway
    through a batch."""
    backend = DuckDBBackend()
    backend.create_staging_table(_ReferencedParentTableCls, duckdb_session)
    staging = _ReferencedParentTableCls.get_staging_table(
        duckdb_session, staging_schema_tag=backend.staging_schema_tag
    )

    duckdb_session.execute(sa.insert(UnreferencedGrandparentTable), [{"id": 1, "name": "g1"}])
    duckdb_session.execute(
        sa.insert(ReferencedParentTable), [{"id": 1, "grandparent_id": 1, "name": "p1"}]
    )
    duckdb_session.execute(sa.insert(ReferencingChildTable), [{"id": 1, "parent_id": 1}])
    duckdb_session.execute(sa.insert(staging), [{"id": 1, "grandparent_id": 1, "name": "p1-updated"}])

    with pytest.raises(ValueError, match="referenced_parent_table"):
        backend.merge_replace(_ReferencedParentTableCls, duckdb_session, ["id"])
