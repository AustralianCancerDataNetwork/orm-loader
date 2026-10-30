"""Helpers that run a column handler on a few values."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy.types import TypeEngine

from orm_loader.conversion import ConversionPolicy


def interpret(
    values: pa.Array | pa.ChunkedArray | list[Any],
    sa_type: TypeEngine[Any],
    policy: ConversionPolicy | None = None,
    *,
    table_name: str | None = None,
    column_name: str | None = None,
) -> tuple[list[Any], list[bool]]:
    """Run the column's handler and return its values and invalid mask as lists."""
    policy = policy or ConversionPolicy()
    arr = values if isinstance(values, (pa.Array, pa.ChunkedArray)) else pa.array(values)
    handler = policy.for_column(sa_type, table_name, column_name)
    out, invalid = handler.interpret(arr, sa_type, policy.nulls)
    return pc.if_else(invalid, pa.scalar(None, out.type), out).to_pylist(), invalid.to_pylist()


def read(raw: Any, sa_type: TypeEngine[Any], policy: ConversionPolicy | None = None, **column: Any) -> Any:
    """Return the value read from one raw value, failing if it is unreadable."""
    values, invalid = interpret([raw], sa_type, policy, **column)
    assert not invalid[0], f"{raw!r} is unreadable as {sa_type!r}"
    return values[0]


def unreadable(raw: Any, sa_type: TypeEngine[Any], policy: ConversionPolicy | None = None, **column: Any) -> bool:
    """Return True when one raw value cannot be read as the column type."""
    return interpret([raw], sa_type, policy, **column)[1][0]
