"""Structured failure context for materialized-view lifecycle operations."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class MaterializationOperation(str, Enum):
    """Lifecycle operation recorded in a failure, for structured handling."""

    CREATE = "create"
    CREATE_INDEX = "create_index"
    REFRESH = "refresh"
    DROP = "drop"


@dataclass(frozen=True, slots=True)
class MaterializationOutcome:
    """Structured context for a successful lifecycle operation."""

    operation: MaterializationOperation
    schema: str | None
    name: str
    index_name: str | None = None


@dataclass(frozen=True, slots=True)
class MaterializationFailure:
    """Structured context for a failed materialized-view operation."""

    operation: MaterializationOperation
    schema: str | None
    name: str
    reason: str
    index_name: str | None = None
    cause: BaseException | None = None


class MaterializationError(RuntimeError):
    """Base exception carrying structured materialization failure context."""

    def __init__(self, failure: MaterializationFailure) -> None:
        self.failure = failure
        qualified_name = (
            f"{failure.schema}.{failure.name}" if failure.schema else failure.name
        )
        super().__init__(
            f"Could not {failure.operation.value} materialized view "
            f"{qualified_name}: {failure.reason}"
        )


class UnsupportedMaterializationDialectError(MaterializationError):
    """Raised before executing Postgres-only DDL/catalog SQL against a
    non-Postgres connection. Defense in depth: the normal ``resolve_backend``
    dispatch path already prevents this via ``_require_capability``; this
    guards direct/manual ``PostgresBackend()`` use."""


class ConcurrentRefreshNotEligibleError(MaterializationError):
    """Raised when concurrent refresh has no declared or live eligible index.

    Declaration errors are detected before execution. Live eligibility is
    owned by PostgreSQL and its rejection is translated into this error.
    """
