from __future__ import annotations

import importlib
import importlib.abc
import sys
from importlib.machinery import ModuleSpec
from types import ModuleType
from typing import TYPE_CHECKING, Sequence, Type, cast, Any

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.engine import Connection, Engine

from orm_loader.backends import BackendCapabilities, DatabaseBackend, resolve_backend

if TYPE_CHECKING:
    from orm_loader.loaders.data_classes import LoaderContext
    from orm_loader.tables.typing import CSVTableProtocol


class _BlockPsycopg(importlib.abc.MetaPathFinder):
    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: ModuleType | None = None,
    ) -> ModuleSpec | None:
        if fullname == "psycopg" or fullname.startswith("psycopg."):
            raise ModuleNotFoundError("No module named 'psycopg'")
        return None


class FakeBackend(DatabaseBackend):
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    @property
    def name(self) -> str:
        return "fake"

    @property
    def dialect_names(self) -> tuple[str, ...]:
        return ("fake",)

    @property
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            supports_fast_load=True,
            supports_fk_toggle=True,
        )

    def create_staging_table(
        self, table_cls: Type[CSVTableProtocol], session: so.Session, staging_name: str
    ) -> None:
        return None

    def drop_staging_table(self, session: so.Session, staging_name: str) -> None:
        return None

    def merge_replace(
        self,
        table_cls: Type[CSVTableProtocol],
        session: so.Session,
        target_name: str,
        staging_name: str,
        pk_cols: list[str],
    ) -> None:
        return None

    def merge_upsert(
        self,
        table_cls: Type[CSVTableProtocol],
        session: so.Session,
        target_name: str,
        staging_name: str,
        pk_cols: list[str],
    ) -> None:
        return None

    def merge_insert(
        self,
        table_cls: Type[CSVTableProtocol],
        session: so.Session,
        target_name: str,
        staging_name: str,
    ) -> None:
        return None

    @staticmethod
    def _normalize_fk_check_state(previous_state: str | int) -> str | int:
        return previous_state

    def disable_fk_check(self, session: so.Session) -> str | int:
        self.calls.append(("disable_fk_check", session))
        return "enabled"

    def enable_fk_check(self, session: so.Session) -> str | int:
        self.calls.append(("enable_fk_check", session))
        return "disabled"

    def restore_fk_check(self, session: so.Session, previous_state: str | int) -> None:
        self.calls.append(("restore_fk_check", previous_state))

    def create_materialized_view(
        self, bind: Engine | Connection, name: str, selectable: sa.sql.Select[Any]
    ) -> None:
        return None

    def refresh_materialized_view(self, bind: Engine | Connection, name: str) -> None:
        return None


class _ComputedTable:
    __table__ = sa.Table(
        "computed_table",
        sa.MetaData(),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String),
        sa.Column("slug", sa.String, sa.Computed("lower(name)")),
    )


_ComputedTableCls = cast("Type[CSVTableProtocol]", _ComputedTable)


def test_backend_capabilities_defaults():
    caps = BackendCapabilities()

    assert caps.supports_fast_load is False
    assert caps.supports_unlogged_staging is False
    assert caps.supports_fk_toggle is False
    assert caps.supports_materialized_views is False


def test_database_backend_is_abstract():
    with pytest.raises(TypeError):
        DatabaseBackend() # type: ignore


def test_fake_backend_can_implement_contract():
    backend = FakeBackend()

    assert backend.name == "fake"
    assert backend.dialect_names == ("fake",)
    assert backend.capabilities.supports_fast_load is True
    assert backend.capabilities.supports_fk_toggle is True
    assert backend.supports_dialect("fake") is True
    assert backend.supports_dialect("sqlite") is False
    assert backend.resolve_index_strategy("auto") == "drop_rebuild"
    assert backend.resolve_index_strategy("keep") == "keep"
    assert backend.load_staging_fast(cast("LoaderContext", None), "staging") is None

    with backend.merge_context(cast("Type[CSVTableProtocol]", None), cast(so.Session, None)):
        pass


def test_require_capability_passes_for_supported_feature():
    backend = FakeBackend()

    backend._require_capability("supports_fast_load", "fast loading")


def test_require_capability_raises_for_unsupported_feature():
    backend = FakeBackend()

    with pytest.raises(NotImplementedError, match="does not support materialized views"):
        backend._require_capability("supports_materialized_views", "materialized views")


def test_require_capability_raises_for_unknown_flag():
    backend = FakeBackend()

    with pytest.raises(AttributeError, match="Unknown backend capability"):
        backend._require_capability("not_a_capability", "something")


def test_resolve_index_strategy_raises_for_invalid_value():
    backend = FakeBackend()

    with pytest.raises(ValueError, match="Unknown index_strategy"):
        backend.resolve_index_strategy("not-valid")


def test_insertable_column_names_exclude_computed_columns():
    backend = FakeBackend()

    assert backend._insertable_column_names(_ComputedTableCls) == ["id", "name"]


def test_bulk_load_context_toggles_fk_and_restores(session):
    backend = FakeBackend()

    with backend.bulk_load_context(session):
        pass

    assert backend.calls == [
        ("disable_fk_check", session),
        ("restore_fk_check", "enabled"),
    ]


def test_bulk_load_context_without_fk_toggle(session):
    backend = FakeBackend()

    with backend.bulk_load_context(session, disable_fk=False):
        pass

    assert backend.calls == []


def test_bulk_load_context_raises_when_capability_missing(session):
    class NoFKBackend(FakeBackend):
        @property
        def capabilities(self) -> BackendCapabilities:
            return BackendCapabilities()

    backend = NoFKBackend()

    with pytest.raises(NotImplementedError, match="does not support foreign key toggling"):
        with backend.bulk_load_context(session):
            pass


def test_bulk_load_context_rolls_back_and_restores(session):
    backend = FakeBackend()

    with pytest.raises(RuntimeError, match="boom"):
        with backend.bulk_load_context(session):
            raise RuntimeError("boom")

    assert backend.calls == [
        ("disable_fk_check", session),
        ("restore_fk_check", "enabled"),
    ]


def test_backends_package_exports():
    import orm_loader.backends as backends

    assert backends.DatabaseBackend is DatabaseBackend
    assert backends.BackendCapabilities is BackendCapabilities
    assert backends.resolve_backend is resolve_backend


def test_resolve_backend_for_sqlite_engine_and_session():
    engine = sa.create_engine("sqlite:///:memory:", future=True)
    session = so.Session(engine)

    try:
        engine_backend = resolve_backend(engine)
        session_backend = resolve_backend(session)

        assert engine_backend.name == "sqlite"
        assert session_backend.name == "sqlite"
    finally:
        session.close()


def test_resolve_backend_raises_for_unknown_dialect():
    class _Unknown:
        class dialect:
            name = "unknown"

    with pytest.raises(NotImplementedError, match="No backend registered"):
        resolve_backend(cast(Engine, _Unknown()))


def test_backends_import_does_not_require_psycopg():
    blocker = _BlockPsycopg()
    original = sys.modules.pop("orm_loader.backends", None)
    sys.meta_path.insert(0, blocker)

    try:
        module = importlib.import_module("orm_loader.backends")
        assert module.DatabaseBackend is not None
    finally:
        sys.meta_path.remove(blocker)
        sys.modules.pop("orm_loader.backends", None)
        if original is not None:
            sys.modules["orm_loader.backends"] = original
