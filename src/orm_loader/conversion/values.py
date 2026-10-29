"""Result type and Arrow helpers shared by the column handlers."""

from __future__ import annotations

from typing import Any, NamedTuple

import pyarrow as pa
import pyarrow.compute as pc

NULL_STRING = pa.scalar(None, pa.string())


class Interpretation(NamedTuple):
    """The outcome of interpreting one column.

    Attributes
    ----------
    values
        Converted values, null where a value is missing or unreadable.
    invalid
        Boolean array that is true where a value could not be read.
    """

    values: pa.Array
    invalid: pa.Array


def as_array(arr: pa.Array | pa.ChunkedArray) -> pa.Array:
    """Return a chunked array as one contiguous array."""
    if isinstance(arr, pa.ChunkedArray):
        return arr.combine_chunks() if arr.num_chunks else pa.array([], arr.type)
    return arr


def is_text_like(arr_type: pa.DataType) -> bool:
    """Return True for string arrays and dictionary arrays of strings."""
    if pa.types.is_dictionary(arr_type):
        arr_type = arr_type.value_type
    return pa.types.is_string(arr_type) or pa.types.is_large_string(arr_type)


def no_failures(length: int) -> pa.Array:
    """Return a boolean array of ``length`` false values."""
    return pa.array([False] * length, pa.bool_())


def cast_each(values: pa.Array, target: pa.DataType) -> tuple[pa.Array, pa.Array]:
    """Cast one element at a time.

    Parameters
    ----------
    values
        Array to cast.
    target
        Arrow type to cast to.

    Returns
    -------
    tuple[pa.Array, pa.Array]
        The cast array with nulls where a cast failed, and a boolean array
        that is true where it failed.
    """
    out: list[Any] = []
    failed: list[bool] = []
    for i in range(len(values)):
        try:
            out.append(pc.cast(values[i : i + 1], target)[0].as_py())
            failed.append(False)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            out.append(None)
            failed.append(True)
    return pa.array(out, target), pa.array(failed, pa.bool_())


def safe_cast(values: pa.Array, target: pa.DataType) -> tuple[pa.Array, pa.Array]:
    """Cast the whole array, falling back to element-wise casts on failure.

    Parameters
    ----------
    values
        Array to cast.
    target
        Arrow type to cast to.

    Returns
    -------
    tuple[pa.Array, pa.Array]
        The cast array with nulls where a cast failed, and a boolean array
        that is true where it failed.
    """
    try:
        return pc.cast(values, target), no_failures(len(values))
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        return cast_each(values, target)
