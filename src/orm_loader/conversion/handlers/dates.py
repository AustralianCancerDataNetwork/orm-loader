"""Handlers for date and timestamp columns, with the date formats and the default date parser."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy.types import TypeEngine

from ..values import NULL_STRING, Interpretation, cast_each, safe_cast
from .base import ConvertingHandler

DateShape = Callable[[pa.Array], tuple[pa.Array, pa.Array]]
DateParser = Callable[[pa.Array, tuple[str, ...]], tuple[pa.Array, pa.Array]]


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
        Shape function by format name.
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

    @classmethod
    def shape(cls, name: str) -> DateShape:
        """Return the shape function for a format name."""
        return {
            "yyyymmdd": cls.yyyymmdd,
            "yyyy-mm-dd": cls.yyyy_mm_dd,
            "dd-mon-yyyy": cls.dd_mon_yyyy,
            "dd/mm/yyyy": cls.dd_mm_yyyy,
            "mm/dd/yyyy": cls.mm_dd_yyyy,
        }[name]

    @classmethod
    def names(cls) -> tuple[str, ...]:
        """Return every supported format name."""
        return ("yyyymmdd", "yyyy-mm-dd", "dd-mon-yyyy", "dd/mm/yyyy", "mm/dd/yyyy")


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
        Ordered names from ``DateFormats.names()``.

    Returns
    -------
    tuple[pa.Array, pa.Array]
        A date32 array with nulls for missing and invalid values, and a
        boolean array that is true where a value is invalid.
    """
    iso = pa.nulls(len(values), pa.string())
    matched = pa.array([False] * len(values), pa.bool_())
    for name in formats:
        ok, candidate = DateFormats.shape(name)(values)
        ok = pc.and_(pc.fill_null(ok, False), pc.is_valid(candidate))
        ok = pc.and_(ok, pc.invert(matched))
        iso = pc.if_else(ok, candidate, iso)
        matched = pc.or_(matched, ok)
    invalid = pc.and_(pc.is_valid(values), pc.invert(matched))
    try:
        return pc.cast(iso, pa.date32()), invalid
    except pa.ArrowInvalid:
        dates, calendar_failed = cast_each(iso, pa.date32())
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
        Ordered names from ``DateFormats.names()``.
    parser
        Replaceable function with the signature of ``parse_dates``.
    """

    distinct: ClassVar[bool] = True

    formats: tuple[str, ...] = DateFormats.DEFAULT
    parser: DateParser = parse_dates

    def __post_init__(self) -> None:
        unknown = [name for name in self.formats if name not in DateFormats.names()]
        if unknown:
            raise ValueError(f"Unknown date formats {unknown}; choose from {sorted(DateFormats.names())}")

    def arrow_type(self, sa_type: TypeEngine[Any]) -> pa.DataType:
        return pa.date32()

    def parse(self, cleaned: pa.Array, sa_type: TypeEngine[Any]) -> Interpretation:
        return Interpretation(*self.parser(cleaned, self.formats))


@dataclass(frozen=True)
class DateTimeHandler(DateHandler):
    """Reads ISO timestamps, and date-only values in the declared date formats.

    A timestamp has date and time separated by ``T`` or a space, with
    optional seconds and up to six fractional digits. A date-only value
    becomes midnight.
    """

    distinct: ClassVar[bool] = False

    ISO: ClassVar[str] = r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?$"

    def arrow_type(self, sa_type: TypeEngine[Any]) -> pa.DataType:
        return pa.timestamp("us")

    def parse(self, cleaned: pa.Array, sa_type: TypeEngine[Any]) -> Interpretation:
        is_iso = pc.fill_null(pc.match_substring_regex(cleaned, self.ISO), False)
        stamps, stamp_failed = safe_cast(pc.if_else(is_iso, cleaned, NULL_STRING), pa.timestamp("us"))
        rest = pc.dictionary_encode(pc.if_else(is_iso, NULL_STRING, cleaned))
        dates, invalid = self.parser(rest.dictionary, self.formats)
        dates = pc.take(dates, rest.indices)
        invalid = pc.fill_null(pc.take(invalid, rest.indices), False)
        values = pc.coalesce(stamps, pc.cast(dates, pa.timestamp("us")))
        return Interpretation(values, pc.or_(invalid, stamp_failed))
