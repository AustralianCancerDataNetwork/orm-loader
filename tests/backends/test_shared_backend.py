from __future__ import annotations

from typing import TYPE_CHECKING, Type, cast

import pytest
import sqlalchemy as sa

from oa_configurator import Dialect
from oa_configurator.testing import DIALECT_PARAMS
from orm_loader.backends import STAGING_SCHEMA, DatabaseBackend
from orm_loader.backends.resolve import resolve_backend
from tests.conftest import session_for
from tests.models import ComputedColumnTable, CompositeTable

if TYPE_CHECKING:
    import sqlalchemy.orm as so

    from orm_loader.tables.typing import FileTableProtocol

_ComputedTableCls = cast("Type[FileTableProtocol]", ComputedColumnTable)
_CompositeTableCls = cast("Type[FileTableProtocol]", CompositeTable)

# merge_replace deletes on sqlite/postgres but updates in place on DuckDB
# (see DuckDBBackend.merge_replace); these tests check the delete-only behavior.
_REPLACE_DELETE_SEMANTICS_PARAMS = tuple(p for p in DIALECT_PARAMS if p.values[0] != Dialect.DUCKDB)


@pytest.fixture(params=DIALECT_PARAMS)
def merge_backend(request: pytest.FixtureRequest) -> tuple[DatabaseBackend, "so.Session"]:
    """Same merge-method contract exercised against every real backend.

    DIALECT_PARAMS carries each dialect's own mark plus `forked` directly
    on the param value, so this still works correctly even though
    request.getfixturevalue(...) is a dynamic, runtime lookup invisible to
    pytest's collection-time fixturenames computation (the usual
    pg_db-in-fixturenames auto-detection can't see it).
    """
    session = session_for(request, request.param)
    staging_schema_tag = STAGING_SCHEMA if request.param != Dialect.SQLITE else None
    return resolve_backend(session, staging_schema_tag=staging_schema_tag), session


@pytest.mark.parametrize("merge_backend", _REPLACE_DELETE_SEMANTICS_PARAMS, indirect=True)
def test_merge_replace_single_pk(merge_backend: tuple[DatabaseBackend, "so.Session"]) -> None:
    backend, session = merge_backend
    backend.create_staging_table(_ComputedTableCls, session)
    staging = _ComputedTableCls.get_staging_table(session, staging_schema_tag=backend.staging_schema_tag)

    session.execute(
        sa.insert(ComputedColumnTable),
        [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}],
    )
    session.execute(sa.insert(staging), [{"id": 1, "name": "alpha-staged"}])

    backend.merge_replace(_ComputedTableCls, session, ["id"])

    remaining = session.execute(sa.select(ComputedColumnTable.id)).scalars().all()
    assert remaining == [2]


@pytest.mark.parametrize("merge_backend", _REPLACE_DELETE_SEMANTICS_PARAMS, indirect=True)
def test_merge_replace_composite_pk(merge_backend: tuple[DatabaseBackend, "so.Session"]) -> None:
    backend, session = merge_backend
    backend.create_staging_table(_CompositeTableCls, session)
    staging = _CompositeTableCls.get_staging_table(session, staging_schema_tag=backend.staging_schema_tag)

    session.execute(
        sa.insert(CompositeTable),
        [{"a": 1, "b": 1, "value": "x"}, {"a": 2, "b": 2, "value": "y"}],
    )
    session.execute(sa.insert(staging), [{"a": 1, "b": 1, "value": "staged"}])

    backend.merge_replace(_CompositeTableCls, session, ["a", "b"])

    remaining = session.execute(sa.select(CompositeTable.a, CompositeTable.b)).all()
    assert remaining == [(2, 2)]


def test_merge_insert_excludes_computed_columns(merge_backend: tuple[DatabaseBackend, "so.Session"]) -> None:
    backend, session = merge_backend
    backend.create_staging_table(_ComputedTableCls, session)
    staging = _ComputedTableCls.get_staging_table(session, staging_schema_tag=backend.staging_schema_tag)
    session.execute(sa.insert(staging), [{"id": 1, "name": "alpha"}])

    backend.merge_insert(_ComputedTableCls, session)

    row = session.execute(sa.select(ComputedColumnTable)).scalars().one()
    assert (row.id, row.name, row.slug) == (1, "alpha", "alpha")


def test_merge_upsert_excludes_computed_columns(merge_backend: tuple[DatabaseBackend, "so.Session"]) -> None:
    backend, session = merge_backend
    backend.create_staging_table(_ComputedTableCls, session)
    staging = _ComputedTableCls.get_staging_table(session, staging_schema_tag=backend.staging_schema_tag)

    session.execute(sa.insert(ComputedColumnTable), [{"id": 1, "name": "existing"}])
    session.execute(
        sa.insert(staging), [{"id": 1, "name": "ignored"}, {"id": 2, "name": "new"}]
    )

    backend.merge_upsert(_ComputedTableCls, session, ["id"])

    rows = {r.id: r.name for r in session.execute(sa.select(ComputedColumnTable)).scalars().all()}
    assert rows == {1: "existing", 2: "new"}


@pytest.mark.postgresql
@pytest.mark.db_dialect
def test_postgres_merge_insert_paginates_rownums_with_initial_gap(pg_session) -> None:
    backend = resolve_backend(pg_session, staging_schema_tag=STAGING_SCHEMA)
    backend.create_staging_table(_ComputedTableCls, pg_session)
    staging = _ComputedTableCls.get_staging_table(
        pg_session, staging_schema_tag=backend.staging_schema_tag
    )
    pg_session.execute(
        sa.insert(staging), [{"id": i, "name": f"row{i}"} for i in range(10)]
    )
    pg_session.execute(
        sa.text(
            'ALTER TABLE "staging"."_staging_computed_column_table" '
            'ALTER COLUMN "_rownum" SET GENERATED BY DEFAULT'
        )
    )
    pg_session.execute(sa.update(staging).values(_rownum=staging.c.id + 10))
    pg_session.execute(
        sa.text(
            'ALTER TABLE "staging"."_staging_computed_column_table" '
            'ALTER COLUMN "_rownum" SET GENERATED ALWAYS'
        )
    )

    backend.merge_insert(_ComputedTableCls, pg_session, merge_batch_size=2)

    ids = pg_session.execute(
        sa.select(ComputedColumnTable.id).order_by(ComputedColumnTable.id)
    ).scalars().all()
    assert ids == list(range(10))


@pytest.mark.parametrize("merge_backend", _REPLACE_DELETE_SEMANTICS_PARAMS, indirect=True)
def test_merge_replace_paginated_path(merge_backend: tuple[DatabaseBackend, "so.Session"]) -> None:
    backend, session = merge_backend
    backend.create_staging_table(_ComputedTableCls, session)
    staging = _ComputedTableCls.get_staging_table(session, staging_schema_tag=backend.staging_schema_tag)

    session.execute(
        sa.insert(ComputedColumnTable), [{"id": i, "name": f"orig{i}"} for i in range(10)]
    )
    session.execute(sa.insert(staging), [{"id": i, "name": f"staged{i}"} for i in range(10)])

    backend.merge_replace(_ComputedTableCls, session, ["id"], merge_batch_size=3)

    remaining = session.execute(
        sa.select(sa.func.count()).select_from(ComputedColumnTable.__table__)
    ).scalar()
    assert remaining == 0


def test_merge_insert_paginated_path(merge_backend: tuple[DatabaseBackend, "so.Session"]) -> None:
    backend, session = merge_backend
    backend.create_staging_table(_ComputedTableCls, session)
    staging = _ComputedTableCls.get_staging_table(session, staging_schema_tag=backend.staging_schema_tag)
    session.execute(sa.insert(staging), [{"id": i, "name": f"row{i}"} for i in range(10)])

    backend.merge_insert(_ComputedTableCls, session, merge_batch_size=3)

    ids = sorted(session.execute(sa.select(ComputedColumnTable.id)).scalars().all())
    assert ids == list(range(10))


def test_merge_upsert_paginated_path(merge_backend: tuple[DatabaseBackend, "so.Session"]) -> None:
    backend, session = merge_backend
    backend.create_staging_table(_ComputedTableCls, session)
    staging = _ComputedTableCls.get_staging_table(session, staging_schema_tag=backend.staging_schema_tag)

    session.execute(
        sa.insert(ComputedColumnTable), [{"id": i, "name": "kept"} for i in range(5)]
    )
    session.execute(
        sa.insert(staging), [{"id": i, "name": "should-not-overwrite"} for i in range(10)]
    )

    backend.merge_upsert(_ComputedTableCls, session, ["id"], merge_batch_size=3)

    rows = {r.id: r.name for r in session.execute(sa.select(ComputedColumnTable)).scalars().all()}
    assert rows == {**{i: "kept" for i in range(5)}, **{i: "should-not-overwrite" for i in range(5, 10)}}
