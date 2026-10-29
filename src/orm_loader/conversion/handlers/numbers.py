"""Handlers for integer, floating point and fixed precision columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy.types import TypeEngine

from ..values import NULL_STRING, Interpretation, safe_cast
from .base import ConvertingHandler


@dataclass(frozen=True)
class NumberHandler(ConvertingHandler):
    """Accepts text matching ``PATTERN`` and casts it to the Arrow type.

    Attributes
    ----------
    PATTERN
        Regular expression a trimmed value must match.
    """

    PATTERN: ClassVar[str]

    def parse(self, cleaned: pa.Array, sa_type: TypeEngine[Any]) -> Interpretation:
        fits = pc.fill_null(pc.match_substring_regex(cleaned, self.PATTERN), False)
        candidate = pc.replace_substring_regex(pc.if_else(fits, cleaned, NULL_STRING), r"^\+", "")
        values, cast_failed = safe_cast(candidate, self.arrow_type(sa_type))
        invalid = pc.and_(pc.is_valid(cleaned), pc.invert(fits))
        return Interpretation(values, pc.or_(invalid, cast_failed))


@dataclass(frozen=True)
class IntegerHandler(NumberHandler):
    """Accepts an optionally signed run of digits."""

    PATTERN: ClassVar[str] = r"^[+-]?\d+$"

    def arrow_type(self, sa_type: TypeEngine[Any]) -> pa.DataType:
        return pa.int64()


@dataclass(frozen=True)
class FloatHandler(NumberHandler):
    """Accepts decimal and exponent forms; infinity is not accepted."""

    PATTERN: ClassVar[str] = r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$"

    def arrow_type(self, sa_type: TypeEngine[Any]) -> pa.DataType:
        return pa.float64()


@dataclass(frozen=True)
class NumericHandler(NumberHandler):
    """Accepts decimal forms and keeps the column's declared precision and scale.

    A value with more digits than the column holds is unreadable. A column
    without a declared precision and scale is read as a float.
    """

    PATTERN: ClassVar[str] = r"^[+-]?(\d+\.?\d*|\.\d+)$"

    def arrow_type(self, sa_type: TypeEngine[Any]) -> pa.DataType:
        precision, scale = getattr(sa_type, "precision", None), getattr(sa_type, "scale", None)
        if precision is not None and scale is not None:
            return pa.decimal128(precision, scale)
        return pa.float64()
