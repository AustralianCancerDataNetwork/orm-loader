from __future__ import annotations

from typing import TYPE_CHECKING, Type, cast

import pytest
import sqlalchemy as sa

from oa_configurator.testing import DIALECT_PARAMS
from orm_loader.backends import STAGING_SCHEMA, DatabaseBackend, PostgresBackend, SQLiteBackend
from tests.models import ComputedColumnTable, CompositeTable

if TYPE_CHECKING:
    import sqlalchemy.orm as so

    from orm_loader.tables.typing import CSVTableProtocol

_ComputedTableCls = cast("Type[CSVTableProtocol]", ComputedColumnTable)
_CompositeTableCls = cast("Type[CSVTableProtocol]", CompositeTable)


@pytest.fixture(params=DIALECT_PARAMS)
def merge_backend(request: pytest.FixtureRequest) -> tuple[DatabaseBackend, "so.Session"]:
    """Same merge-method contract exercised against both real backends.
    Only the postgresql param ever requests pg_session, so the sqlite
    param never needs a database.

    DIALECT_PARAMS carries each dialect's own mark plus `forked` directly
    on the param value, so this still works correctly even though
    request.getfixturevalue("pg_session") is a dynamic, runtime lookup
    invisible to pytest's collection-time fixturenames computation (the
    usual pg_db-in-fixturenames auto-detection can't see it).
    """
    if request.param == "postgresql":
        session = request.getfixturevalue("pg_session")
        return PostgresBackend(staging_schema=STAGING_SCHEMA), session
    session = request.getfixturevalue("session")
    return SQLiteBackend(), session


def test_merge_replace_single_pk(merge_backend: tuple[DatabaseBackend, "so.Session"]) -> None:
    backend, session = merge_backend
    backend.create_staging_table(_ComputedTableCls, session)
    staging = _ComputedTableCls.get_staging_table(session, staging_schema=backend.staging_schema)

    session.execute(
        sa.insert(ComputedColumnTable),
        [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}],
    )
    session.execute(sa.insert(staging), [{"id": 1, "name": "alpha-staged"}])

    backend.merge_replace(_ComputedTableCls, session, ["id"])

    remaining = session.execute(sa.select(ComputedColumnTable.id)).scalars().all()
    assert remaining == [2]


def test_merge_replace_composite_pk(merge_backend: tuple[DatabaseBackend, "so.Session"]) -> None:
    backend, session = merge_backend
    backend.create_staging_table(_CompositeTableCls, session)
    staging = _CompositeTableCls.get_staging_table(session, staging_schema=backend.staging_schema)

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
    staging = _ComputedTableCls.get_staging_table(session, staging_schema=backend.staging_schema)
    session.execute(sa.insert(staging), [{"id": 1, "name": "alpha"}])

    backend.merge_insert(_ComputedTableCls, session)

    row = session.execute(sa.select(ComputedColumnTable)).scalars().one()
    assert (row.id, row.name, row.slug) == (1, "alpha", "alpha")


def test_merge_upsert_excludes_computed_columns(merge_backend: tuple[DatabaseBackend, "so.Session"]) -> None:
    backend, session = merge_backend
    backend.create_staging_table(_ComputedTableCls, session)
    staging = _ComputedTableCls.get_staging_table(session, staging_schema=backend.staging_schema)

    session.execute(sa.insert(ComputedColumnTable), [{"id": 1, "name": "existing"}])
    session.execute(
        sa.insert(staging), [{"id": 1, "name": "ignored"}, {"id": 2, "name": "new"}]
    )

    backend.merge_upsert(_ComputedTableCls, session, ["id"])

    rows = {r.id: r.name for r in session.execute(sa.select(ComputedColumnTable)).scalars().all()}
    assert rows == {1: "existing", 2: "new"}


def test_merge_replace_paginated_path(merge_backend: tuple[DatabaseBackend, "so.Session"]) -> None:
    backend, session = merge_backend
    backend.create_staging_table(_ComputedTableCls, session)
    staging = _ComputedTableCls.get_staging_table(session, staging_schema=backend.staging_schema)

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
    staging = _ComputedTableCls.get_staging_table(session, staging_schema=backend.staging_schema)
    session.execute(sa.insert(staging), [{"id": i, "name": f"row{i}"} for i in range(10)])

    backend.merge_insert(_ComputedTableCls, session, merge_batch_size=3)

    ids = sorted(session.execute(sa.select(ComputedColumnTable.id)).scalars().all())
    assert ids == list(range(10))


def test_merge_upsert_paginated_path(merge_backend: tuple[DatabaseBackend, "so.Session"]) -> None:
    backend, session = merge_backend
    backend.create_staging_table(_ComputedTableCls, session)
    staging = _ComputedTableCls.get_staging_table(session, staging_schema=backend.staging_schema)

    session.execute(
        sa.insert(ComputedColumnTable), [{"id": i, "name": "kept"} for i in range(5)]
    )
    session.execute(
        sa.insert(staging), [{"id": i, "name": "should-not-overwrite"} for i in range(10)]
    )

    backend.merge_upsert(_ComputedTableCls, session, ["id"], merge_batch_size=3)

    rows = {r.id: r.name for r in session.execute(sa.select(ComputedColumnTable)).scalars().all()}
    assert rows == {**{i: "kept" for i in range(5)}, **{i: "should-not-overwrite" for i in range(5, 10)}}
