from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.exc import UnsupportedCompilationError

from orm_loader.materialized_views import (
    MaterializedSelectable,
    MaterializedViewIndex,
    MaterializedViewSpec,
)
from orm_loader.mappers.materialised_view_contracts import (
    CreateMaterializedViewIndex,
    DropMaterializedView,
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


def test_materialized_view_spec_implements_public_protocol():
    spec = MaterializedViewSpec(
        name="mv_test",
        selectable=sa.select(sa.literal(1).label("row_id")),
        logical_identity=("row_id",),
    )

    assert isinstance(spec, MaterializedSelectable)


def test_materialized_view_spec_requires_a_selected_logical_identity():
    selectable = sa.select(sa.literal(1).label("row_id"))

    with pytest.raises(ValueError, match="logical_identity must not be empty"):
        MaterializedViewSpec(name="mv_test", selectable=selectable, logical_identity=())
    with pytest.raises(ValueError, match="logical_identity columns are not selected"):
        MaterializedViewSpec(
            name="mv_test",
            selectable=selectable,
            logical_identity=("missing",),
        )


def test_materialized_view_spec_validates_indexes_and_dependencies():
    selectable = sa.select(sa.literal(1).label("row_id"))
    index = MaterializedViewIndex(name="mv_test_uq", columns=("row_id",), unique=True)

    with pytest.raises(ValueError, match="index names must not contain duplicates"):
        MaterializedViewSpec(
            name="mv_test",
            selectable=selectable,
            logical_identity=("row_id",),
            indexes=(index, index),
        )
    with pytest.raises(ValueError, match="columns are not selected"):
        MaterializedViewSpec(
            name="mv_test",
            selectable=selectable,
            logical_identity=("row_id",),
            indexes=(MaterializedViewIndex(name="bad", columns=("missing",)),),
        )
    with pytest.raises(ValueError, match="cannot depend on itself"):
        MaterializedViewSpec(
            name="mv_test",
            selectable=selectable,
            logical_identity=("row_id",),
            dependencies=("mv_test",),
        )
    with pytest.raises(ValueError, match="dependencies must not contain duplicates"):
        MaterializedViewSpec(
            name="mv_test",
            selectable=selectable,
            logical_identity=("row_id",),
            dependencies=("source", "source"),
        )


def test_drop_materialized_view_default_compiles_with_if_exists():
    ddl = DropMaterializedView("mv_test", schema="reporting")

    assert str(ddl.compile(dialect=_DIALECT)) == (
        'DROP MATERIALIZED VIEW IF EXISTS "reporting"."mv_test"'
    )


def test_drop_materialized_view_if_exists_false_and_cascade():
    ddl = DropMaterializedView(
        "mv_test", schema="reporting", if_exists=False, cascade=True
    )

    assert str(ddl.compile(dialect=_DIALECT)) == (
        'DROP MATERIALIZED VIEW "reporting"."mv_test" CASCADE'
    )


def test_create_materialized_view_index_compiles_unique_index_ddl():
    index = MaterializedViewIndex(name="mv_test_id_uq", columns=("id", "person_id"), unique=True)
    ddl = CreateMaterializedViewIndex("mv_test", index, schema="reporting")

    assert str(ddl.compile(dialect=_DIALECT)) == (
        'CREATE UNIQUE INDEX "mv_test_id_uq" '
        'ON "reporting"."mv_test" ("id", "person_id")'
    )


def test_create_materialized_view_index_non_unique_and_if_not_exists_false():
    index = MaterializedViewIndex(name="mv_test_id_idx", columns=("id",))
    ddl = CreateMaterializedViewIndex("mv_test", index, schema="reporting")

    assert str(ddl.compile(dialect=_DIALECT)) == (
        'CREATE INDEX "mv_test_id_idx" ON "reporting"."mv_test" ("id")'
    )


def test_materialized_view_ddl_is_postgresql_only():
    ddl = DropMaterializedView("mv_test")

    with pytest.raises(UnsupportedCompilationError):
        ddl.compile(dialect=sqlite.dialect())


def test_create_materialized_view_quotes_target_and_is_postgresql_only():
    ddl = CreateMaterializedView(
        'view"name',
        sa.select(sa.literal(1).label("row_id")),
        schema="analysis space",
    )

    compiled = str(ddl.compile(dialect=_DIALECT))
    assert compiled.startswith(
        'CREATE MATERIALIZED VIEW "analysis space"."view""name" AS SELECT'
    )
    assert "IF NOT EXISTS" not in compiled
    with pytest.raises(UnsupportedCompilationError):
        ddl.compile(dialect=sqlite.dialect())
