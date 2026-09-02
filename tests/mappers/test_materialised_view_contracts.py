from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from orm_loader.mappers.materialised_view_contracts import (
    CreateMaterializedViewIndex,
    DropMaterializedView,
    MaterializedViewIndex,
)
from orm_loader.mappers.materialised_view_mixin import CreateMaterializedView

_DIALECT = postgresql.dialect()


def test_materialized_view_index_requires_a_non_empty_name():
    with pytest.raises(ValueError, match="index name"):
        MaterializedViewIndex(name="", columns=("id",))


def test_materialized_view_index_requires_at_least_one_column():
    with pytest.raises(ValueError, match="index columns must not be empty"):
        MaterializedViewIndex(name="idx", columns=())


def test_materialized_view_index_rejects_empty_column_name():
    with pytest.raises(ValueError, match="index column"):
        MaterializedViewIndex(name="idx", columns=("id", ""))


def test_materialized_view_index_rejects_duplicate_columns():
    with pytest.raises(ValueError, match="duplicates"):
        MaterializedViewIndex(name="idx", columns=("id", "id"))


def test_materialized_view_index_defaults_to_non_unique():
    index = MaterializedViewIndex(name="idx", columns=("id",))

    assert index.unique is False


def test_drop_materialized_view_default_compiles_with_if_exists():
    ddl = DropMaterializedView("reporting.mv_test")

    assert str(ddl.compile(dialect=_DIALECT)) == "DROP MATERIALIZED VIEW IF EXISTS reporting.mv_test"


def test_drop_materialized_view_if_exists_false_and_cascade():
    ddl = DropMaterializedView("reporting.mv_test", if_exists=False, cascade=True)

    assert str(ddl.compile(dialect=_DIALECT)) == "DROP MATERIALIZED VIEW reporting.mv_test CASCADE"


def test_create_materialized_view_index_compiles_unique_index_ddl():
    index = MaterializedViewIndex(name="mv_test_id_uq", columns=("id", "person_id"), unique=True)
    ddl = CreateMaterializedViewIndex("reporting.mv_test", index)

    assert str(ddl.compile(dialect=_DIALECT)) == (
        'CREATE UNIQUE INDEX IF NOT EXISTS "mv_test_id_uq" '
        'ON reporting.mv_test ("id", "person_id")'
    )


def test_create_materialized_view_index_non_unique_and_if_not_exists_false():
    index = MaterializedViewIndex(name="mv_test_id_idx", columns=("id",))
    ddl = CreateMaterializedViewIndex("reporting.mv_test", index, if_not_exists=False)

    assert str(ddl.compile(dialect=_DIALECT)) == (
        'CREATE INDEX "mv_test_id_idx" ON reporting.mv_test ("id")'
    )


def test_create_materialized_view_options_compile_conditionally():
    ddl = CreateMaterializedView(
        "mv_test",
        sa.select(sa.literal(1).label("id")),
        with_data=False,
        if_not_exists=False,
    )

    assert str(ddl.compile(dialect=_DIALECT)) == (
        "CREATE MATERIALIZED VIEW mv_test as SELECT 1 AS id WITH NO DATA"
    )
