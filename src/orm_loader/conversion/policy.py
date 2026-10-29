"""The settings of a load: what is missing, what happens on error, and how each type is read."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal

import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy.types import (
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    Numeric,
    String,
    TypeEngine,
)

from .handlers import (
    BooleanHandler,
    ColumnHandler,
    DateHandler,
    DateTimeHandler,
    FloatHandler,
    IntegerHandler,
    NumericHandler,
    PassthroughHandler,
    TextHandler,
)
from .values import no_failures


@dataclass(frozen=True)
class NullPolicy:
    """What counts as a missing value, shared by every column handler.

    Text columns keep every nonempty value verbatim apart from the markers.
    Typed columns treat empty text and the typed missing words as missing.

    Attributes
    ----------
    empty_is_null
        Empty and whitespace-only text becomes null in text columns.
    text_markers
        Exact, case-sensitive text values treated as missing in text columns.
        Matching ignores surrounding whitespace.
    marker_quoting
        ``"ignore"`` matches markers whether or not the source quoted them.
        ``"unquoted_only"`` matches only markers the source did not quote,
        which needs a source that records quoting and import-level markers.
    typed_missing_words
        Lowercase words treated as missing in non-text columns.
    """

    DEFAULT_TYPED_MISSING: ClassVar[frozenset[str]] = frozenset(
        {"null", "none", "na", "n/a", "nan", "#n/a", "-nan", "<na>"}
    )

    empty_is_null: bool = True
    text_markers: frozenset[str] = frozenset()
    marker_quoting: Literal["ignore", "unquoted_only"] = "ignore"
    typed_missing_words: frozenset[str] = DEFAULT_TYPED_MISSING

    def __post_init__(self) -> None:
        if self.marker_quoting not in ("ignore", "unquoted_only"):
            raise ValueError(f"marker_quoting must be 'ignore' or 'unquoted_only', not {self.marker_quoting!r}")

    def text_missing(self, trimmed: pa.Array, markers: frozenset[str] | None = None) -> pa.Array:
        """Return a mask of trimmed text values that are missing in a text column.

        Parameters
        ----------
        trimmed
            String array without surrounding whitespace.
        markers
            Markers that replace ``text_markers``, or None to use them.

        Returns
        -------
        pa.Array
            Boolean array that is true where the value is missing.
        """
        words = self.text_markers if markers is None else markers
        if self.marker_quoting != "ignore":
            words = frozenset()
        return self._mask(trimmed, words, empty=self.empty_is_null, casefold=False)

    def typed_missing(self, trimmed: pa.Array) -> pa.Array:
        """Return a mask of trimmed text values that are missing in a typed column."""
        return self._mask(trimmed, self.typed_missing_words, empty=True, casefold=True)

    @staticmethod
    def _mask(trimmed: pa.Array, words: frozenset[str], *, empty: bool, casefold: bool) -> pa.Array:
        mask = no_failures(len(trimmed))
        if empty:
            mask = pc.or_(mask, pc.fill_null(pc.equal(pc.utf8_length(trimmed), 0), False))
        if words:
            probe = pc.utf8_lower(trimmed) if casefold else trimmed
            hit = pc.is_in(probe, value_set=pa.array(sorted(words), pa.string()))
            mask = pc.or_(mask, pc.fill_null(hit, False))
        return mask


@dataclass(frozen=True)
class ErrorPolicy:
    """What happens to a row with an unreadable value or a missing required value.

    Attributes
    ----------
    on_error
        ``"raise"`` aborts on the first batch containing such a row.
        ``"reject_row"`` drops those rows and writes them to a rejects file.
    max_rejects
        With ``reject_row``, abort once rejected rows exceed this count, or
        this fraction of rows read when it is a float between 0 and 1.
    """

    on_error: Literal["raise", "reject_row"] = "raise"
    max_rejects: float | None = None

    def __post_init__(self) -> None:
        if self.on_error not in ("raise", "reject_row"):
            raise ValueError(f"on_error must be 'raise' or 'reject_row', not {self.on_error!r}")


@dataclass(frozen=True)
class ConversionPolicy:
    """The settings of one load, shared by every column.

    The handler for a column is, in order, the entry in ``columns`` and the
    handler for the most specific class of the column type among
    ``handlers`` and the built-in fields.

    Attributes
    ----------
    nulls
        What counts as a missing value.
    errors
        What happens to unreadable and missing required values.
    text
        Handler for string and text columns.
    integer
        Handler for integer columns.
    floating
        Handler for floating point columns.
    numeric
        Handler for fixed precision columns.
    boolean
        Handler for boolean columns.
    date
        Handler for date columns.
    datetime
        Handler for timestamp columns.
    handlers
        Handlers for further column types, or replacements for the built-in
        ones, by SQLAlchemy type class.
    columns
        Handlers for single columns by ``(table_name, column_name)``.
    """

    nulls: NullPolicy = field(default_factory=NullPolicy)
    errors: ErrorPolicy = field(default_factory=ErrorPolicy)
    text: TextHandler = field(default_factory=TextHandler)
    integer: IntegerHandler = field(default_factory=IntegerHandler)
    floating: FloatHandler = field(default_factory=FloatHandler)
    numeric: NumericHandler = field(default_factory=NumericHandler)
    boolean: BooleanHandler = field(default_factory=BooleanHandler)
    date: DateHandler = field(default_factory=DateHandler)
    datetime: DateTimeHandler = field(default_factory=DateTimeHandler)
    handlers: Mapping[type, ColumnHandler] = field(default_factory=dict)
    columns: Mapping[tuple[str, str], ColumnHandler] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.nulls.marker_quoting == "unquoted_only":
            texts = [self.text, *self.columns.values(), *self.handlers.values()]
            if any(isinstance(h, TextHandler) and h.markers is not None for h in texts):
                raise ValueError("marker_quoting='unquoted_only' supports NullPolicy.text_markers only")

    def for_column(
        self, sa_type: TypeEngine[Any], table_name: str | None = None, column_name: str | None = None
    ) -> ColumnHandler:
        """Return the handler that interprets a column.

        Parameters
        ----------
        sa_type
            Column type.
        table_name
            Table the column belongs to.
        column_name
            The column.

        Returns
        -------
        ColumnHandler
            The handler for the column; a passthrough handler for a column
            type with none.
        """
        if table_name and column_name:
            handler = self.columns.get((table_name, column_name))
            if handler is not None:
                return handler
        # Dispatch default first and then overwrite with any registered handlers
        by_type: dict[type, ColumnHandler] = {
            String: self.text,
            Integer: self.integer,
            Float: self.floating,
            Numeric: self.numeric,
            Boolean: self.boolean,
            Date: self.date,
            DateTime: self.datetime,
            **self.handlers,
        }
        for cls in type(sa_type).__mro__:
            if cls in by_type:
                return by_type[cls]
        return PassthroughHandler()
