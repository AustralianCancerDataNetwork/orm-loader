"""Declarative contracts and PostgreSQL DDL for materialized views."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql.compiler import DDLCompiler
from sqlalchemy.sql.selectable import SelectBase


def _require_identifier(value: str, *, field_name: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty")


def _require_distinct(values: tuple[str, ...], *, field_name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} must not contain duplicates")


@dataclass(frozen=True, slots=True)
class MaterializedViewIndex:
    """A simple column index declared on a materialized view.

    Only plain column indexes are representable here (no expressions or
    partial predicates). That restriction is deliberate: it's what keeps
    "is this index eligible for CONCURRENTLY refresh" decidable from the
    declaration alone, without inspecting the live catalog first.
    """

    name: str
    columns: tuple[str, ...]
    unique: bool = False

    def __post_init__(self) -> None:
        _require_identifier(self.name, field_name="index name")
        if not self.columns:
            raise ValueError("index columns must not be empty")
        for column in self.columns:
            _require_identifier(column, field_name="index column")
        _require_distinct(self.columns, field_name="index columns")


@runtime_checkable
class MaterializedSelectable(Protocol):
    """Definition consumed by materialized-view lifecycle orchestration."""

    @property
    def name(self) -> str: ...

    @property
    def selectable(self) -> SelectBase: ...

    @property
    def logical_identity(self) -> tuple[str, ...]: ...

    @property
    def dependencies(self) -> tuple[str, ...]: ...

    @property
    def indexes(self) -> tuple[MaterializedViewIndex, ...]: ...


@dataclass(frozen=True, slots=True)
class MaterializedViewSpec:
    """Immutable view definition with an explicit logical row identity.

    The identity is metadata rather than a database constraint. Applications
    use it to declare the view's grain and normally enforce it with a matching
    unique index or a release-time uniqueness assertion.
    """

    name: str
    selectable: SelectBase
    logical_identity: tuple[str, ...]
    dependencies: tuple[str, ...] = ()
    indexes: tuple[MaterializedViewIndex, ...] = ()

    def __post_init__(self) -> None:
        _require_identifier(self.name, field_name="materialized view name")
        if not self.logical_identity:
            raise ValueError("logical_identity must not be empty")
        for column in self.logical_identity:
            _require_identifier(column, field_name="logical identity column")
        _require_distinct(self.logical_identity, field_name="logical_identity")

        output_columns = set(self.selectable.selected_columns.keys())
        unknown_identity = sorted(set(self.logical_identity) - output_columns)
        if unknown_identity:
            raise ValueError(
                f"logical_identity columns are not selected: {unknown_identity}"
            )

        index_names = tuple(index.name for index in self.indexes)
        _require_distinct(index_names, field_name="index names")
        for index in self.indexes:
            unknown_index_columns = sorted(set(index.columns) - output_columns)
            if unknown_index_columns:
                raise ValueError(
                    f"index {index.name!r} columns are not selected: "
                    f"{unknown_index_columns}"
                )

        for dependency in self.dependencies:
            _require_identifier(dependency, field_name="dependency")
        _require_distinct(self.dependencies, field_name="dependencies")
        if self.name in self.dependencies:
            raise ValueError("a materialized view cannot depend on itself")

    @property
    def concurrent_refresh_indexes(self) -> tuple[MaterializedViewIndex, ...]:
        """Declared indexes whose simple shape can support concurrency."""
        return tuple(index for index in self.indexes if index.unique)


def _qualified_target(ddl_compiler: DDLCompiler, name: str, schema: str | None) -> str:
    """Quote target components with the dialect compiling the statement."""
    preparer = ddl_compiler.preparer
    quoted_name = preparer.quote_identifier(name)
    if schema is None:
        return quoted_name
    return f"{preparer.quote_identifier(schema)}.{quoted_name}"


class DropMaterializedView(DDLElement):
    """Drop one materialized view.

    Parameters
    ----------
    name
        Unquoted materialized-view name. The compiler quotes it.
    schema
        Optional unquoted schema name. The compiler quotes it separately.
    if_exists
        Emit ``IF EXISTS`` so dropping an already-absent view is a no-op
        rather than an error.
    cascade
        Emit ``CASCADE`` to also drop objects that depend on this view.
    """

    inherit_cache = False

    def __init__(
        self,
        name: str,
        *,
        schema: str | None = None,
        if_exists: bool = True,
        cascade: bool = False,
    ) -> None:
        self.name = name
        self.schema = schema
        self.if_exists = if_exists
        self.cascade = cascade


@compiler.compiles(DropMaterializedView, "postgresql")
def _drop_materialized_view(
    element: DropMaterializedView,
    compiler: DDLCompiler,
    **kwargs: Any,
) -> str:
    existence = "IF EXISTS " if element.if_exists else ""
    cascade = " CASCADE" if element.cascade else ""
    target = _qualified_target(compiler, element.name, element.schema)
    return f"DROP MATERIALIZED VIEW {existence}{target}{cascade}"


class CreateMaterializedViewIndex(DDLElement):
    """Create one declared index on a materialized view.

    Parameters
    ----------
    name
        Unquoted materialized-view name. The compiler quotes it.
    index
        The index to create.
    if_not_exists
        Emit ``IF NOT EXISTS`` so creating an already-present index is a
        no-op rather than an error.
    """

    def __init__(
        self,
        name: str,
        index: MaterializedViewIndex,
        *,
        schema: str | None = None,
        if_not_exists: bool = False,
    ) -> None:
        self.name = name
        self.schema = schema
        self.index = index
        self.if_not_exists = if_not_exists

    inherit_cache = False


@compiler.compiles(CreateMaterializedViewIndex, "postgresql")
def _create_materialized_view_index(
    element: CreateMaterializedViewIndex,
    compiler: DDLCompiler,
    **kwargs: Any,
) -> str:
    preparer = compiler.preparer
    index_name = preparer.quote_identifier(element.index.name)
    columns = ", ".join(preparer.quote_identifier(c) for c in element.index.columns)
    uniqueness = "UNIQUE " if element.index.unique else ""
    existence = "IF NOT EXISTS " if element.if_not_exists else ""
    target = _qualified_target(compiler, element.name, element.schema)
    return f"CREATE {uniqueness}INDEX {existence}{index_name} ON {target} ({columns})"
