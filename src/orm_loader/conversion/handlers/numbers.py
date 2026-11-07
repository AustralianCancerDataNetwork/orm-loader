"""Handlers for integer, floating point and fixed precision columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, cast

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
        candidate = pc.replace_substring_regex(
            pc.if_else(fits, cleaned, NULL_STRING), self.STRIPPED, ""
        )
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
        bits = (
            64
            if isinstance(sa_type, BigInteger)
            else 16
            if isinstance(sa_type, SmallInteger)
            else 32
        )
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
    """Reads plain and exponent decimals into an Arrow decimal with the column's precision and scale.

    A precision without a scale has scale 0. The column must declare a
    precision of at most 38 digits, the width of an Arrow decimal. Text is
    handed to Arrow's decimal parser only after ``normalised`` has checked
    that the column holds it exactly, because that parser silently wraps or
    underflows on very long digit strings and very small exponents. Every
    other value is unreadable.

    Attributes
    ----------
    MAX_PRECISION
        Widest precision an Arrow decimal holds, and the most digits its
        parser accumulates without wrapping.
    SHAPE
        Regular expression splitting a value into sign, whole digits,
        fraction digits and an exponent of at most three digits.
    """

    MAX_PRECISION: ClassVar[int] = 38
    SHAPE: ClassVar[str] = (
        r"^(?P<sign>[+-]?)(?P<whole>\d*)(?:\.(?P<frac>\d*))?(?:[eE](?P<exp>[+-]?\d{1,3}))?$"
    )

    @staticmethod
    def precision_scale(sa_type: TypeEngine[Any]) -> tuple[int | None, int | None]:
        """Return a column's declared precision and scale."""
        return getattr(sa_type, "precision", None), getattr(sa_type, "scale", None)

    def validate(self, sa_type: TypeEngine[Any]) -> None:
        precision, scale = self.precision_scale(sa_type)
        if (
            precision is None
            or not 1 <= precision <= self.MAX_PRECISION
            or not 0 <= (scale or 0) <= precision
        ):
            raise ValueError(
                f"Numeric needs a precision from 1 to {self.MAX_PRECISION} and a scale from 0 to the precision, "
                f"declared ({precision}, {scale}). A PassthroughHandler for the column leaves the value to the database"
            )

    def arrow_type(self, sa_type: TypeEngine[Any]) -> pa.DataType:
        precision, scale = self.precision_scale(sa_type)
        return pa.decimal128(cast(int, precision), scale or 0)

    def normalised(self, cleaned: pa.Array, precision: int, scale: int) -> pa.Array:
        """Return the text Arrow converts, null where the column cannot hold the value exactly.

        A value is kept when it has a digit and fits the column without
        rounding or overflow. It must also stay inside what Arrow's parser
        reads exactly: its digits, scaled to the column, and its implied
        scale are at most ``MAX_PRECISION``. A zero is kept as ``0``, and
        a leading plus sign is dropped.

        Parameters
        ----------
        cleaned
            Trimmed text with missing values set to null.
        precision
            Precision of the column.
        scale
            Scale of the column.

        Returns
        -------
        pa.Array
            String array, null for missing and for unreadable values.
        """
        shaped, digits, fraction, exponent = self._split(cleaned)
        count = pc.utf8_length(digits)
        significant = pc.utf8_length(pc.utf8_ltrim(digits, "0"))
        trailing = pc.subtract(count, pc.utf8_length(pc.utf8_rtrim(digits, "0")))
        implied = pc.subtract(fraction, exponent)
        floor = pa.scalar(0, pa.int32())
        needed = pc.max_element_wise(pc.subtract(implied, trailing), floor)
        growth = pc.max_element_wise(
            pc.subtract(pc.add(pa.scalar(scale, pa.int32()), exponent), fraction), floor
        )
        holds = pc.and_(
            pc.less_equal(needed, scale),
            pc.less_equal(pc.subtract(significant, implied), precision - scale),
        )
        parsable = pc.and_(
            pc.less_equal(pc.add(count, growth), self.MAX_PRECISION),
            pc.less_equal(implied, self.MAX_PRECISION),
        )
        readable = pc.and_(
            pc.and_(shaped, pc.greater_equal(count, 1)),
            pc.or_(pc.equal(significant, 0), pc.and_(holds, parsable)),
        )
        text = pc.if_else(
            pc.equal(significant, 0), "0", pc.replace_substring_regex(cleaned, r"^\+", "")
        )
        return pc.if_else(pc.fill_null(readable, False), text, NULL_STRING)

    def _split(self, cleaned: pa.Array) -> tuple[pa.Array, pa.Array, pa.Array, pa.Array]:
        """Split each value into its shape mask, digits, fraction length and exponent."""
        parts = pc.extract_regex(cleaned, self.SHAPE)

        def group(name: str) -> pa.Array:
            return pc.fill_null(pc.struct_field(parts, name), "")

        exponent = pc.replace_substring_regex(group("exp"), r"^\+", "")
        exponent = pc.if_else(pc.equal(exponent, ""), NULL_STRING, exponent).cast(pa.int32())
        digits = pc.binary_join_element_wise(group("whole"), group("frac"), "")
        return pc.is_valid(parts), digits, pc.utf8_length(group("frac")), pc.fill_null(exponent, 0)

    def parse(self, cleaned: pa.Array, sa_type: TypeEngine[Any]) -> Interpretation:
        precision, scale = self.precision_scale(sa_type)
        candidate = self.normalised(cleaned, cast(int, precision), scale or 0)
        values, cast_failed = safe_cast(candidate, self.arrow_type(sa_type))
        invalid = pc.and_(pc.is_valid(cleaned), pc.is_null(candidate))
        return Interpretation(values, pc.or_(invalid, cast_failed))

    def _interpret_typed(self, arr: pa.Array, sa_type: TypeEngine[Any]) -> Interpretation:
        """Read typed input as exactly as the text a writer would print for it.

        Integers are widened first because Arrow casts only to a wide enough
        decimal. A float is read as its shortest decimal text, so a Parquet
        float column loads the same values as a CSV of the same numbers.
        Not-a-number is missing and infinity is unreadable.
        """
        if pa.types.is_integer(arr.type):
            arr = pc.cast(arr, pa.decimal128(self.MAX_PRECISION, 0))
        elif pa.types.is_floating(arr.type):
            arr = arr.cast(pa.float32()) if pa.types.is_float16(arr.type) else arr
            arr = pc.if_else(pc.fill_null(pc.is_nan(arr), False), pa.scalar(None, arr.type), arr)
            return self.parse(pc.cast(arr, pa.string()), sa_type)
        return super()._interpret_typed(arr, sa_type)
