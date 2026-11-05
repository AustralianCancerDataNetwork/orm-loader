"""Result type and Arrow helpers shared by the column handlers."""

from __future__ import annotations

from typing import NamedTuple

import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy.types import TypeDecorator, TypeEngine

NULL_STRING = pa.scalar(None, pa.string())


class Interpretation(NamedTuple):
    """The outcome of interpreting one column.

    Attributes
    ----------
    values
        Converted values, null where a value is missing. Where ``invalid`` is
        true the value is not meaningful and is not used.
    invalid
        Boolean array that is true where a value could not be read.
    """

    values: pa.Array
    invalid: pa.Array


def underlying_type(sa_type: TypeEngine) -> TypeEngine:
    """Return the type a ``TypeDecorator`` stores its values as, or the type itself."""
    while isinstance(sa_type, TypeDecorator):
        sa_type = sa_type.impl
    return sa_type


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
    return pa.repeat(False, length)


def safe_cast(values: pa.Array, target: pa.DataType) -> tuple[pa.Array, pa.Array]:
    """Cast an array, isolating the values that cannot be cast.

    The whole array is cast first. When that fails it is split in halves and
    each half is cast the same way, so few unreadable values cost few casts.

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
        if len(values) == 1:
            return pa.nulls(1, target), pa.array([True])
        middle = len(values) // 2
        head, head_failed = safe_cast(values.slice(0, middle), target)
        tail, tail_failed = safe_cast(values.slice(middle), target)
        return pa.concat_arrays([head, tail]), pa.concat_arrays([head_failed, tail_failed])
