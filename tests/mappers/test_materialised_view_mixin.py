from __future__ import annotations

import importlib
from typing import Any

import pytest
import sqlalchemy as sa

from orm_loader.mappers.materialised_view_contracts import MaterializedViewIndex
from orm_loader.mappers.materialised_view_mixin import (
    MaterializedViewMixin,
    refresh_all_mvs,
    resolve_mv_refresh_order,
)


class _FakeBackend:
    """Capture lifecycle calls without touching a real database."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def create_materialized_view(self, bind, name, selectable, **kwargs):
        self.calls.append(("create_materialized_view", (bind, name, selectable), kwargs))

    def create_materialized_view_index(self, bind, name, index, **kwargs):
        self.calls.append(("create_materialized_view_index", (bind, name, index), kwargs))

    def refresh_materialized_view(self, bind, name, **kwargs):
        self.calls.append(("refresh_materialized_view", (bind, name), kwargs))

    def drop_materialized_view(self, bind, name, **kwargs):
        self.calls.append(("drop_materialized_view", (bind, name), kwargs))


@pytest.fixture
def fake_backend(monkeypatch: pytest.MonkeyPatch) -> _FakeBackend:
    backend = _FakeBackend()
    resolver_module = importlib.import_module("orm_loader.backends.resolve")
    monkeypatch.setattr(resolver_module, "resolve_backend", lambda bind: backend)
    return backend


_SELECT = sa.select(sa.literal(1).label("row_id"))
_INDEX = MaterializedViewIndex(name="mv_test_row_id_uq", columns=("row_id",), unique=True)


class _NoIndexMv(MaterializedViewMixin):
    __mv_name__ = "mv_no_index"
    __mv_select__ = _SELECT


class _IndexedMv(MaterializedViewMixin):
    __mv_name__ = "mv_indexed"
    __mv_select__ = _SELECT
    __mv_indexes__ = (_INDEX,)


def test_create_mv_forwards_default_args_to_backend(fake_backend: _FakeBackend):
    _NoIndexMv.create_mv("bind")

    assert fake_backend.calls == [
        (
            "create_materialized_view",
            ("bind", "mv_no_index", _SELECT),
            {"schema": None, "with_data": True, "if_not_exists": True},
        )
    ]


def test_create_mv_creates_declared_indexes_after_the_view(fake_backend: _FakeBackend):
    _IndexedMv.create_mv("bind")

    assert [call[0] for call in fake_backend.calls] == [
        "create_materialized_view",
        "create_materialized_view_index",
    ]
    assert fake_backend.calls[1][1] == ("bind", "mv_indexed", _INDEX)
    assert fake_backend.calls[1][2] == {"schema": None, "if_not_exists": True}


def test_create_mv_create_indexes_false_skips_index_creation(fake_backend: _FakeBackend):
    _IndexedMv.create_mv("bind", create_indexes=False)

    assert [call[0] for call in fake_backend.calls] == ["create_materialized_view"]


def test_create_mv_forwards_schema_with_data_and_if_not_exists_overrides(fake_backend: _FakeBackend):
    _NoIndexMv.create_mv("bind", schema="reporting", with_data=False, if_not_exists=False)

    assert fake_backend.calls[0][2] == {
        "schema": "reporting",
        "with_data": False,
        "if_not_exists": False,
    }


def test_create_mv_forwards_if_not_exists_to_declared_indexes(fake_backend: _FakeBackend):
    _IndexedMv.create_mv("bind", if_not_exists=False)

    assert fake_backend.calls[1][2] == {"schema": None, "if_not_exists": False}


def test_create_mv_engine_uses_one_transaction_for_view_and_indexes(monkeypatch: pytest.MonkeyPatch):
    engine = sa.create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(
            sa.text("CREATE TABLE materialization_steps (step TEXT NOT NULL)")
        )

    connections: list[sa.engine.Connection] = []
    fail_once = True

    class TransactionalBackend(_FakeBackend):
        def create_materialized_view(self, bind, name, selectable, **kwargs):
            connections.append(bind)
            bind.execute(
                sa.text("INSERT INTO materialization_steps (step) VALUES ('view')")
            )

        def create_materialized_view_index(self, bind, name, index, **kwargs):
            nonlocal fail_once
            connections.append(bind)
            bind.execute(
                sa.text("INSERT INTO materialization_steps (step) VALUES ('index')")
            )
            if fail_once:
                fail_once = False
                raise RuntimeError("index failed")

    backend = TransactionalBackend()
    resolver_module = importlib.import_module("orm_loader.backends.resolve")
    monkeypatch.setattr(resolver_module, "resolve_backend", lambda bind: backend)

    class TransactionalMv(MaterializedViewMixin):
        __mv_name__ = "mv_transactional"
        __mv_select__ = _SELECT
        __mv_indexes__ = (MaterializedViewIndex("mv_transactional_idx", ("row_id",)),)

    with pytest.raises(RuntimeError, match="index failed"):
        TransactionalMv.create_mv(engine)

    assert connections[0] is connections[1]
    with engine.connect() as connection:
        assert connection.execute(
            sa.text("SELECT COUNT(*) FROM materialization_steps")
        ).scalar_one() == 0

    TransactionalMv.create_mv(engine)
    with engine.connect() as connection:
        assert connection.execute(
            sa.text("SELECT step FROM materialization_steps ORDER BY rowid")
        ).all() == [("view",), ("index",)]


def test_refresh_mv_forwards_default_args_and_declared_indexes(fake_backend: _FakeBackend):
    _IndexedMv.refresh_mv("bind")

    assert fake_backend.calls == [
        (
            "refresh_materialized_view",
            ("bind", "mv_indexed"),
            {"schema": None, "concurrently": False, "declared_indexes": (_INDEX,)},
        )
    ]


def test_refresh_mv_forwards_schema_and_concurrently(fake_backend: _FakeBackend):
    _IndexedMv.refresh_mv("bind", schema="reporting", concurrently=True)

    assert fake_backend.calls[0][2] == {
        "schema": "reporting",
        "concurrently": True,
        "declared_indexes": (_INDEX,),
    }


def test_drop_mv_forwards_default_args(fake_backend: _FakeBackend):
    _NoIndexMv.drop_mv("bind")

    assert fake_backend.calls == [
        (
            "drop_materialized_view",
            ("bind", "mv_no_index"),
            {"schema": None, "if_exists": True, "cascade": False},
        )
    ]


def test_drop_mv_forwards_schema_if_exists_and_cascade(fake_backend: _FakeBackend):
    _NoIndexMv.drop_mv("bind", schema="reporting", if_exists=False, cascade=True)

    assert fake_backend.calls[0][2] == {
        "schema": "reporting",
        "if_exists": False,
        "cascade": True,
    }


class _MvA(MaterializedViewMixin):
    __mv_name__ = "mv_a"
    __mv_select__ = _SELECT


class _MvB(MaterializedViewMixin):
    __mv_name__ = "mv_b"
    __mv_select__ = _SELECT
    __mv_dependencies__ = {"mv_a"}


class _MvC(MaterializedViewMixin):
    __mv_name__ = "mv_c"
    __mv_select__ = _SELECT
    __mv_dependencies__ = {"mv_a", "mv_b"}


def test_resolve_mv_refresh_order_linear_dependency():
    assert resolve_mv_refresh_order([_MvB, _MvA]) == [_MvA, _MvB]


def test_resolve_mv_refresh_order_diamond_dependency():
    ordered = resolve_mv_refresh_order([_MvC, _MvA, _MvB])

    assert ordered.index(_MvA) < ordered.index(_MvB) < ordered.index(_MvC)


def test_resolve_mv_refresh_order_ignores_dependencies_outside_the_given_set():
    class _StandaloneMv(MaterializedViewMixin):
        __mv_name__ = "mv_standalone"
        __mv_select__ = _SELECT
        __mv_dependencies__ = {"some_other_table_not_in_this_registry"}

    assert resolve_mv_refresh_order([_StandaloneMv]) == [_StandaloneMv]


def test_resolve_mv_refresh_order_raises_on_cycle():
    class _CycleA(MaterializedViewMixin):
        __mv_name__ = "mv_cycle_a"
        __mv_select__ = _SELECT
        __mv_dependencies__ = {"mv_cycle_b"}

    class _CycleB(MaterializedViewMixin):
        __mv_name__ = "mv_cycle_b"
        __mv_select__ = _SELECT
        __mv_dependencies__ = {"mv_cycle_a"}

    with pytest.raises(RuntimeError, match="Cycle detected"):
        resolve_mv_refresh_order([_CycleA, _CycleB])


def test_refresh_all_mvs_refreshes_in_dependency_order(fake_backend: _FakeBackend):
    refresh_all_mvs("bind", [_MvB, _MvA])

    refreshed_names = [call[1][1] for call in fake_backend.calls]
    assert refreshed_names == ["mv_a", "mv_b"]
