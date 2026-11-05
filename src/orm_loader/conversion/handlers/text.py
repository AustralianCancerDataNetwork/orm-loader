"""Handler for string and text columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy.types import TypeEngine

from ..values import Interpretation, no_failures
from .base import ConvertingHandler

if TYPE_CHECKING:
    from ..policy import NullPolicy


@dataclass(frozen=True)
class TextHandler(ConvertingHandler):
    """Keeps every nonempty value verbatim and rejects values longer than the column.

    Attributes
    ----------
    strip
        Surrounding whitespace is removed from stored text.
    """

    strip: bool = True

    def arrow_type(self, sa_type: TypeEngine[Any]) -> pa.DataType:
        return pa.string()

    def clean(self, raw: pa.Array, trimmed: pa.Array) -> pa.Array:
        return trimmed if self.strip else raw

    def missing(self, trimmed: pa.Array, nulls: NullPolicy) -> pa.Array:
        return nulls.text_missing(trimmed)

    def parse(self, cleaned: pa.Array, sa_type: TypeEngine[Any]) -> Interpretation:
        return Interpretation(cleaned, no_failures(len(cleaned)))

    def check(self, values: pa.Array, sa_type: TypeEngine[Any]) -> pa.Array | None:
        length = getattr(sa_type, "length", None)
        if not length:
            return None
        return pc.fill_null(pc.greater(pc.utf8_length(values), length), False)


@dataclass(frozen=True)
class EnumHandler(TextHandler):
    """Keeps text verbatim and rejects values that are not a label the column stores."""

    def check(self, values: pa.Array, sa_type: TypeEngine[Any]) -> pa.Array | None:
        known = pc.is_in(values, value_set=pa.array(sa_type.enums, pa.string()))
        return pc.and_(pc.is_valid(values), pc.invert(known))
