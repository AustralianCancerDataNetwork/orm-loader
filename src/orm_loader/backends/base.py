from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Type, Any, Iterator

import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.engine import Connection, Engine

if TYPE_CHECKING:
    from ..loaders.data_classes import LoaderContext
    from ..tables.typing import CSVTableProtocol


@dataclass(frozen=True)
class BackendCapabilities:
    """
    Capability flags exposed by a database backend.

    These defaults are intentionally conservative. Concrete backends should
    opt into capabilities explicitly.
    """

    supports_fast_load: bool = False
    supports_unlogged_staging: bool = False
    supports_fk_toggle: bool = False
    supports_materialized_views: bool = False


class Dialect(str, Enum):
    """Supported SQLAlchemy dialect names."""

    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"


class DatabaseBackend(ABC):
    """
    Abstract base class for database-specific loader behavior.

    This class defines the stable contract for future backend implementations
    without changing existing loader orchestration yet.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable backend name."""

    @property
    @abstractmethod
    def dialect(self) -> Dialect:
        """SQLAlchemy dialect handled by this backend."""

    @property
    @abstractmethod
    def capabilities(self) -> BackendCapabilities:
        """Capability flags supported by this backend."""

    def supports_dialect(self, dialect: Dialect) -> bool:
        """Return ``True`` when the backend handles the given dialect."""
        return self.dialect == dialect

    @property
    def default_index_strategy(self) -> str:
        """Default index strategy used when callers request ``auto``."""
        return "drop_rebuild"

    def resolve_index_strategy(self, index_strategy: str) -> str:
        """
        Resolve a caller-facing index strategy to a concrete backend choice.
        """
        valid = {"auto", "drop_rebuild", "keep"}
        if index_strategy not in valid:
            raise ValueError(
                f"Unknown index_strategy '{index_strategy}'. Expected one of: {sorted(valid)}"
            )
        if index_strategy == "auto":
            return self.default_index_strategy
        return index_strategy

    def _require_capability(self, capability_name: str, feature_name: str) -> None:
        """
        Raise a clear error when a backend capability is not supported.
        """
        if not hasattr(self.capabilities, capability_name):
            raise AttributeError(
                f"Unknown backend capability {capability_name!r} on {type(self.capabilities).__name__}"
            )
        if not getattr(self.capabilities, capability_name):
            raise NotImplementedError(
                f"Backend '{self.name}' does not support {feature_name}"
            )
        
    @contextmanager
    def _as_connection(
        self,
        bind: Engine | Connection,
    ) -> Iterator[Connection]:
        if isinstance(bind, Engine):
            with bind.begin() as conn:
                yield conn
        else:
            yield bind

    def _insertable_column_names(
        self,
        table_cls: Type["CSVTableProtocol"],
    ) -> list[str]:
        """
        Return column names safe to include in generic insert statements.

        Computed columns are excluded because backend loaders and merge helpers
        should not attempt to write to them directly.
        """
        return [c.name for c in table_cls.__table__.columns if c.computed is None]

    @abstractmethod
    def create_staging_table(
        self,
        table_cls: Type["CSVTableProtocol"],
        session: so.Session,
        staging_name: str,
    ) -> None:
        """Create a staging table for the supplied ORM table class."""

    @abstractmethod
    def drop_staging_table(
        self,
        session: so.Session,
        staging_name: str,
    ) -> None:
        """Drop a staging table if it exists."""

    def load_staging_fast(
        self,
        loader_context: "LoaderContext",
        staging_name: str,
    ) -> int | None:
        """
        Attempt a backend-native fast-path load.

        Return the inserted row count when handled, or ``None`` when the
        backend has no fast-path loader for the given context.
        """
        return None

    @staticmethod
    @abstractmethod
    def _normalize_fk_check_state(previous_state: str | int) -> str | int:
        """Validate and normalise a previously-returned FK state before interpolating into SQL.

        Each backend accepts a different type (SQLite: int, Postgres: str) and must
        implement this to guard restore_fk_check() against invalid or injected values.
        """

    @abstractmethod
    def disable_fk_check(self, session: so.Session) -> str | int:
        """Disable FK checks and return the previous backend-specific state."""

    @abstractmethod
    def enable_fk_check(self, session: so.Session) -> str | int:
        """Explicitly enable FK checks and return the previous backend-specific state."""

    @abstractmethod
    def restore_fk_check(
        self,
        session: so.Session,
        previous_state: str | int,
    ) -> None:
        """Restore FK checks to a previously returned backend-specific state."""

    @abstractmethod
    def merge_replace(
        self,
        table_cls: Type["CSVTableProtocol"],
        session: so.Session,
        target_name: str,
        staging_name: str,
        pk_cols: list[str],
        *,
        merge_batch_size: int | None = None,
    ) -> None:
        """Merge staging rows by replacing matching target rows first."""

    @abstractmethod
    def merge_upsert(
        self,
        table_cls: Type["CSVTableProtocol"],
        session: so.Session,
        target_name: str,
        staging_name: str,
        pk_cols: list[str],
        *,
        merge_batch_size: int | None = None,
    ) -> None:
        """Merge staging rows using backend-specific upsert semantics."""

    @abstractmethod
    def merge_insert(
        self,
        table_cls: Type["CSVTableProtocol"],
        session: so.Session,
        target_name: str,
        staging_name: str,
        *,
        merge_batch_size: int | None = None,
    ) -> None:
        """Insert all staging rows into the target table."""

    def merge_context(
        self,
        table_cls: Type["CSVTableProtocol"],
        session: so.Session,
    ) -> AbstractContextManager[None]:
        """Return a context manager for merge-time backend operations."""
        return nullcontext()

    @contextmanager
    def bulk_load_context(
        self,
        session: so.Session,
        *,
        disable_fk: bool = True,
        no_autoflush: bool = True,
    ):
        """
        Generic bulk-load context that defers FK semantics to the backend.
        """
        previous_fk_state: str | int | None = None
        try:
            if disable_fk:
                self._require_capability("supports_fk_toggle", "foreign key toggling")
                raw_state = self.disable_fk_check(session)
                previous_fk_state = self._normalize_fk_check_state(raw_state)

            if no_autoflush:
                with session.no_autoflush:
                    yield
            else:
                yield

        except Exception:
            session.rollback()
            raise

        finally:
            if previous_fk_state is not None:
                self.restore_fk_check(session, previous_fk_state)

    @abstractmethod
    def create_materialized_view(
        self,
        bind: "Engine | Connection",
        name: str,
        selectable: sa.sql.Select[Any],
    ) -> None:
        """Create a materialized view for the supplied selectable."""

    @abstractmethod
    def refresh_materialized_view(
        self,
        bind: "Engine | Connection",
        name: str,
    ) -> None:
        """Refresh a materialized view."""
