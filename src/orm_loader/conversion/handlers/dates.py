"""Handlers for date and timestamp columns, with the date formats and the date parser."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, ClassVar

import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy.types import TypeEngine

from ..values import NULL_STRING, Interpretation, no_failures, safe_cast
from .base import ConvertingHandler

DateShape = Callable[[pa.Array], tuple[pa.Array, pa.Array]]


class DateFormats:
    """Supported date format names, each a shape check plus ISO reassembly.

    A shape function takes trimmed strings and returns a boolean array that
    is true where the string fits the format, and the same strings
    reassembled as ISO dates.

    Attributes
    ----------
    DEFAULT
        Mutually distinguishable formats tried in order when none is declared.
    MONTHS
        Lowercase month abbreviations used by ``dd-mon-yyyy``.
    SHAPES
        Read-only shape function by format name.
    """

    DEFAULT: ClassVar[tuple[str, ...]] = ("yyyymmdd", "dd-mon-yyyy", "yyyy-mm-dd")
    MONTHS: ClassVar[tuple[str, ...]] = (
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    )

    @staticmethod
    def yyyymmdd(values: pa.Array) -> tuple[pa.Array, pa.Array]:
        ok = pc.and_(pc.equal(pc.utf8_length(values), 8), pc.utf8_is_digit(values))
        cut = pc.utf8_slice_codeunits
        return ok, pc.binary_join_element_wise(cut(values, 0, 4), cut(values, 4, 6), cut(values, 6, 8), "-")

    @staticmethod
    def yyyy_mm_dd(values: pa.Array) -> tuple[pa.Array, pa.Array]:
        return pc.match_substring_regex(values, r"^\d{4}-\d{2}-\d{2}$"), values

    @staticmethod
    def dd_mon_yyyy(values: pa.Array) -> tuple[pa.Array, pa.Array]:
        ok = pc.match_substring_regex(values, r"^\d{2}-[A-Za-z]{3}-\d{4}$")
        cut = pc.utf8_slice_codeunits
        month = pc.index_in(pc.utf8_lower(cut(values, 3, 6)), value_set=pa.array(DateFormats.MONTHS))
        month = pc.utf8_lpad(pc.cast(pc.add(month, 1), pa.string()), 2, "0")
        return ok, pc.binary_join_element_wise(cut(values, 7, 11), month, cut(values, 0, 2), "-")

    @staticmethod
    def dd_mm_yyyy(values: pa.Array) -> tuple[pa.Array, pa.Array]:
        ok = pc.match_substring_regex(values, r"^\d{2}/\d{2}/\d{4}$")
        cut = pc.utf8_slice_codeunits
        return ok, pc.binary_join_element_wise(cut(values, 6, 10), cut(values, 3, 5), cut(values, 0, 2), "-")

    @staticmethod
    def mm_dd_yyyy(values: pa.Array) -> tuple[pa.Array, pa.Array]:
        ok = pc.match_substring_regex(values, r"^\d{2}/\d{2}/\d{4}$")
        cut = pc.utf8_slice_codeunits
        return ok, pc.binary_join_element_wise(cut(values, 6, 10), cut(values, 0, 2), cut(values, 3, 5), "-")

    SHAPES: ClassVar[Mapping[str, DateShape]] = MappingProxyType(
        {
            "yyyymmdd": yyyymmdd,
            "yyyy-mm-dd": yyyy_mm_dd,
            "dd-mon-yyyy": dd_mon_yyyy,
            "dd/mm/yyyy": dd_mm_yyyy,
            "mm/dd/yyyy": mm_dd_yyyy,
        }
    )


def parse_dates(values: pa.Array, formats: tuple[str, ...]) -> tuple[pa.Array, pa.Array]:
    """Parse trimmed distinct date strings against the declared formats.

    A value is matched to the first format whose shape it fits, reassembled
    as an ISO string and validated by Arrow's string-to-date cast, so no
    calendar logic lives here. Null inputs stay null and are not invalid.

    Parameters
    ----------
    values
        String array with missing values already set to null.
    formats
        Ordered keys of ``DateFormats.SHAPES``.

    Returns
    -------
    tuple[pa.Array, pa.Array]
        A date32 array with nulls for missing and invalid values, and a
        boolean array that is true where a value is invalid.
    """
    iso = pa.nulls(len(values), pa.string())
    matched = no_failures(len(values))
    for name in formats:
        ok, candidate = DateFormats.SHAPES[name](values)
        ok = pc.and_(pc.fill_null(ok, False), pc.is_valid(candidate))
        ok = pc.and_(ok, pc.invert(matched))
        iso = pc.if_else(ok, candidate, iso)
        matched = pc.or_(matched, ok)
    invalid = pc.and_(pc.is_valid(values), pc.invert(matched))
    dates, calendar_failed = safe_cast(iso, pa.date32())
    return dates, pc.or_(invalid, calendar_failed)


@dataclass(frozen=True)
class DateHandler(ConvertingHandler):
    """Parses dates against an ordered list of declared formats.

    Values are parsed once per distinct value. A value that fits none of the
    formats, or is not a calendar date, is unreadable. Forms such as
    ``02/01/2020`` are never guessed and need a declared format.

    Attributes
    ----------
    formats
        Ordered keys of ``DateFormats.SHAPES``.
    """

    distinct: ClassVar[bool] = True

    formats: tuple[str, ...] = DateFormats.DEFAULT

    def __post_init__(self) -> None:
        unknown = [name for name in self.formats if name not in DateFormats.SHAPES]
        if unknown:
            raise ValueError(f"Unknown date formats {unknown}; choose from {sorted(DateFormats.SHAPES)}")

    def arrow_type(self, sa_type: TypeEngine[Any]) -> pa.DataType:
        return pa.date32()

    def parse(self, cleaned: pa.Array, sa_type: TypeEngine[Any]) -> Interpretation:
        return Interpretation(*parse_dates(cleaned, self.formats))

    def check(self, values: pa.Array, sa_type: TypeEngine[Any]) -> pa.Array | None:
        return pc.fill_null(pc.less(pc.year(values), 1), False)


@dataclass(frozen=True)
class DateTimeHandler(DateHandler):
    """Reads ISO timestamps, and date-only values in the declared date formats.

    - A timestamp has date and time separated by ``T`` or a space, with
    optional seconds and up to six fractional digits.
    -  A date-only value becomes midnight. 
    - The column type decides about time zones: 
        - ``DateTime(timezone=True)`` column requires an offset (``Z`` or
    ``+hh:mm``) and stores UTC, and 
        - a column without a time zone rejects an offset. 
    - A date-only value is not read for a time zone column. 
    - Typed input follows the same rule: a time zone aware timestamp does not fill a
    naive column and a naive one does not fill an aware column.
    """

    distinct: ClassVar[bool] = False

    _STAMP = r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?"
    NAIVE: ClassVar[str] = _STAMP + "$"
    AWARE: ClassVar[str] = _STAMP + r"(Z|[+-]\d{2}:\d{2})$"

    def arrow_type(self, sa_type: TypeEngine[Any]) -> pa.DataType:
        return pa.timestamp("us", "UTC" if getattr(sa_type, "timezone", False) else None)

    def expected(self, sa_type: TypeEngine[Any]) -> str:
        return "DateTime with an offset (Z or +hh:mm)" if getattr(sa_type, "timezone", False) else "DateTime without an offset"

    def _interpret_typed(self, arr: pa.Array, sa_type: TypeEngine[Any]) -> Interpretation:
        target = self.arrow_type(sa_type)
        stored_aware = pa.types.is_timestamp(arr.type) and arr.type.tz is not None
        if not pa.types.is_null(arr.type) and stored_aware != (target.tz is not None):
            return Interpretation(pa.nulls(len(arr), target), pc.is_valid(arr))
        return super()._interpret_typed(arr, sa_type)

    def parse(self, cleaned: pa.Array, sa_type: TypeEngine[Any]) -> Interpretation:
        target = self.arrow_type(sa_type)
        aware = target.tz is not None
        is_iso = pc.fill_null(pc.match_substring_regex(cleaned, self.AWARE if aware else self.NAIVE), False)
        stamps, stamp_failed = safe_cast(pc.if_else(is_iso, cleaned, NULL_STRING), target)
        if aware:
            return Interpretation(stamps, pc.or_(stamp_failed, pc.and_(pc.is_valid(cleaned), pc.invert(is_iso))))
        rest = pc.dictionary_encode(pc.if_else(is_iso, NULL_STRING, cleaned))
        dates, invalid = parse_dates(rest.dictionary, self.formats)
        dates = pc.take(dates, rest.indices)
        invalid = pc.fill_null(pc.take(invalid, rest.indices), False)
        values = pc.coalesce(stamps, pc.cast(dates, target))
        return Interpretation(values, pc.or_(invalid, stamp_failed))
