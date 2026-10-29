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
    markers
        Exact, case-sensitive values treated as missing, ignoring surrounding
        whitespace. Replaces ``NullPolicy.text_markers`` for the columns this
        handler serves; None uses the policy markers.
    """

    strip: bool = True
    markers: frozenset[str] | None = None

    def arrow_type(self, sa_type: TypeEngine[Any]) -> pa.DataType:
        return pa.string()

    def clean(self, raw: pa.Array, trimmed: pa.Array) -> pa.Array:
        return trimmed if self.strip else raw

    def missing(self, trimmed: pa.Array, nulls: NullPolicy) -> pa.Array:
        return nulls.text_missing(trimmed, self.markers)

    def parse(self, cleaned: pa.Array, sa_type: TypeEngine[Any]) -> Interpretation:
        return Interpretation(cleaned, no_failures(len(cleaned)))

    def check(self, values: pa.Array, sa_type: TypeEngine[Any]) -> pa.Array | None:
        length = getattr(sa_type, "length", None)
        if not length:
            return None
        return pc.fill_null(pc.greater(pc.utf8_length(values), length), False)
