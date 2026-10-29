"""Handlers for integer, floating point and fixed precision columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy.types import BigInteger, SmallInteger, TypeEngine

from ..values import NULL_STRING, Interpretation, safe_cast
from .base import ConvertingHandler


@dataclass(frozen=True)
class NumberHandler(ConvertingHandler):
    """Accepts text matching ``PATTERN`` and casts it to the Arrow type.

    Attributes
    ----------
    PATTERN
        Regular expression a trimmed value must match, unless ``pattern``
        is overridden.
    STRIPPED
        Regular expression for the parts of a matching value removed before
        the cast.
    """

    PATTERN: ClassVar[str]
    STRIPPED: ClassVar[str] = r"^\+"

    def pattern(self, sa_type: TypeEngine[Any]) -> str:
        """Return the regular expression a trimmed value must match for the column."""
        return self.PATTERN

    def parse(self, cleaned: pa.Array, sa_type: TypeEngine[Any]) -> Interpretation:
        fits = pc.fill_null(pc.match_substring_regex(cleaned, self.pattern(sa_type)), False)
        candidate = pc.replace_substring_regex(pc.if_else(fits, cleaned, NULL_STRING), self.STRIPPED, "")
        values, cast_failed = safe_cast(candidate, self.arrow_type(sa_type))
        invalid = pc.and_(pc.is_valid(cleaned), pc.invert(fits))
        return Interpretation(values, pc.or_(invalid, cast_failed))


@dataclass(frozen=True)
class IntegerHandler(NumberHandler):
    """Accepts an optionally signed run of digits, with an optional all-zero fraction.

    A value is unreadable when it does not fit the column: 16 bits for
    ``SmallInteger``, 64 for ``BigInteger`` and 32 for any other integer.
    """

    PATTERN: ClassVar[str] = r"^[+-]?\d+(\.0*)?$"
    STRIPPED: ClassVar[str] = r"^\+|\.0*$"

    def arrow_type(self, sa_type: TypeEngine[Any]) -> pa.DataType:
        return pa.int64()

    def check(self, values: pa.Array, sa_type: TypeEngine[Any]) -> pa.Array | None:
        low, high = self.bounds(sa_type)
        return pc.fill_null(pc.or_(pc.less(values, low), pc.greater(values, high)), False)

    @staticmethod
    def bounds(sa_type: TypeEngine[Any]) -> tuple[int, int]:
        """Return the smallest and largest value the column type holds."""
        bits = 64 if isinstance(sa_type, BigInteger) else 16 if isinstance(sa_type, SmallInteger) else 32
        return -(1 << (bits - 1)), (1 << (bits - 1)) - 1


@dataclass(frozen=True)
class FloatHandler(NumberHandler):
    """Accepts decimal and exponent forms; infinity, written or overflowing, is not accepted."""

    PATTERN: ClassVar[str] = r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$"

    def arrow_type(self, sa_type: TypeEngine[Any]) -> pa.DataType:
        return pa.float64()

    def check(self, values: pa.Array, sa_type: TypeEngine[Any]) -> pa.Array | None:
        return pc.fill_null(pc.is_inf(values), False)


@dataclass(frozen=True)
class NumericHandler(NumberHandler):
    """Accepts decimal forms and keeps the column's declared precision and scale.

    A precision without a scale has scale 0. A column without a precision is
    read as ``decimal128(38, 18)``. Trailing zeros of the fraction are
    dropped, and a value with more digits than the column holds is unreadable.
    """

    @staticmethod
    def precision_scale(sa_type: TypeEngine[Any]) -> tuple[int, int]:
        """Return the precision and scale a column is read with."""
        precision = getattr(sa_type, "precision", None)
        if precision is None:
            return 38, 18
        return precision, getattr(sa_type, "scale", None) or 0

    def arrow_type(self, sa_type: TypeEngine[Any]) -> pa.DataType:
        return pa.decimal128(*self.precision_scale(sa_type))

    def clean(self, raw: pa.Array, trimmed: pa.Array) -> pa.Array:
        return pc.replace_substring_regex(trimmed, r"(\.\d*?)0+$", r"\1")

    def pattern(self, sa_type: TypeEngine[Any]) -> str:
        """Return the decimal forms that fit the column's integer and fraction digits.

        Arrow wraps around instead of failing when a string has more digits
        than a decimal holds, so both parts are bounded here.
        """
        precision, scale = self.precision_scale(sa_type)
        forms = [rf"0*\d{{0,{precision - scale}}}(\.\d{{0,{scale}}})?"]
        if scale:
            forms.append(rf"\.\d{{1,{scale}}}")
        return rf"^[+-]?({'|'.join(forms)})$"
