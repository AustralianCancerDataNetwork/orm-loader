"""Declarative index contract and DDL elements extending materialized-view support.

Schema qualification for these DDL elements follows the same convention as
``CreateMaterializedView``: callers must build a fully qualified, quoted
target string themselves (see ``orm_loader.helpers.sql.qualify_identifier``)
before constructing one of these elements. The compiler has no live bindable
to qualify a bare name itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql.compiler import DDLCompiler


def _require_identifier(value: str, *, field_name: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty")


@dataclass(frozen=True)
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
        if len(set(self.columns)) != len(self.columns):
            raise ValueError("index columns must not contain duplicates")


class DropMaterializedView(DDLElement):
    """Drop one materialized view.

    Parameters
    ----------
    name
        Fully qualified, quoted name of the materialized view to drop (see
        ``orm_loader.helpers.sql.qualify_identifier``).
    if_exists
        Emit ``IF EXISTS`` so dropping an already-absent view is a no-op
        rather than an error.
    cascade
        Emit ``CASCADE`` to also drop objects that depend on this view.
    """

    def __init__(self, name: str, *, if_exists: bool = True, cascade: bool = False) -> None:
        self.name = name
        self.if_exists = if_exists
        self.cascade = cascade


@compiler.compiles(DropMaterializedView)
def _drop_materialized_view(
    element: DropMaterializedView,
    compiler: DDLCompiler,
    **kwargs: Any,
) -> str:
    existence = "IF EXISTS " if element.if_exists else ""
    cascade = " CASCADE" if element.cascade else ""
    return f"DROP MATERIALIZED VIEW {existence}{element.name}{cascade}"


class CreateMaterializedViewIndex(DDLElement):
    """Create one declared index on a materialized view.

    Parameters
    ----------
    target
        Fully qualified, quoted name of the materialized view to index (see
        ``orm_loader.helpers.sql.qualify_identifier``).
    index
        The index to create.
    if_not_exists
        Emit ``IF NOT EXISTS`` so creating an already-present index is a
        no-op rather than an error.
    """

    def __init__(
        self,
        target: str,
        index: MaterializedViewIndex,
        *,
        if_not_exists: bool = True,
    ) -> None:
        self.target = target
        self.index = index
        self.if_not_exists = if_not_exists


@compiler.compiles(CreateMaterializedViewIndex)
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
    return f"CREATE {uniqueness}INDEX {existence}{index_name} ON {element.target} ({columns})"
