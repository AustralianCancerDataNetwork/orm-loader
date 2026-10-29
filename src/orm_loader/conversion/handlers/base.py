"""The contract every column handler fulfils, and the staged template most handlers share."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy.types import TypeEngine

from ..values import NULL_STRING, Interpretation, as_array, is_text_like, no_failures, safe_cast

if TYPE_CHECKING:
    from ..policy import NullPolicy

class ColumnHandler(ABC):
    """Interprets the raw values of the columns of one kind of column type.

    A handler is a frozen settings object. It is selected for a column by
    ``ConversionPolicy.for_column`` and holds no state about the column.
    """

    @abstractmethod
    def interpret(
        self, arr: pa.Array | pa.ChunkedArray, sa_type: TypeEngine[Any], nulls: NullPolicy
    ) -> Interpretation:
        """Interpret a raw column without raising on unreadable values.

        Parameters
        ----------
        arr
            Raw column.
        sa_type
            Column type the values are converted to.
        nulls
            What counts as a missing value.

        Returns
        -------
        Interpretation
            Converted values with nulls where a value is missing or
            unreadable, and a mask that is true where a value was unreadable.
        """

    def expected(self, sa_type: TypeEngine[Any]) -> str:
        """Return the name reported in an error for an unreadable value."""
        return type(sa_type).__name__


class PassthroughHandler(ColumnHandler):
    """Returns a column unchanged, for column types that are not converted."""

    def interpret(
        self, arr: pa.Array | pa.ChunkedArray, sa_type: TypeEngine[Any], nulls: NullPolicy
    ) -> Interpretation:
        arr = as_array(arr)
        return Interpretation(arr, no_failures(len(arr)))


class ConvertingHandler(ColumnHandler):
    """Converts a column in four stages: clean, detect missing, parse, check.

    Text input follows the stages. Input that already has a type, such as a
    Parquet column, is cast directly with floating NaN read as missing. A
    value that does not survive the cast back to its input type is lossy and
    unreadable, and ``check`` runs on the cast values.
    Subclasses supply ``arrow_type`` and ``parse`` and may override ``clean``,
    ``missing`` and ``check``.

    Attributes
    ----------
    distinct
        Run the stages once per distinct value and gather the results back,
        for types whose parse step is expensive per value.
    """

    distinct: ClassVar[bool] = False

    @abstractmethod
    def arrow_type(self, sa_type: TypeEngine[Any]) -> pa.DataType:
        """Return the Arrow type of the converted values."""

    @abstractmethod
    def parse(self, cleaned: pa.Array, sa_type: TypeEngine[Any]) -> Interpretation:
        """Parse cleaned text in which missing values are null.

        Parameters
        ----------
        cleaned
            String array with missing values set to null.
        sa_type
            Column type the values are converted to.

        Returns
        -------
        Interpretation
            Parsed values and a mask of values that did not fit.
        """

    def clean(self, raw: pa.Array, trimmed: pa.Array) -> pa.Array:
        """Return the text that is parsed, the trimmed text by default."""
        return trimmed

    def missing(self, trimmed: pa.Array, nulls: NullPolicy) -> pa.Array:
        """Return a mask of values that count as missing, by the typed rules by default."""
        return nulls.typed_missing(trimmed)

    def check(self, values: pa.Array, sa_type: TypeEngine[Any]) -> pa.Array | None:
        """Return a mask of parsed values the column cannot hold, or None when it holds any."""
        return None

    def interpret(
        self, arr: pa.Array | pa.ChunkedArray, sa_type: TypeEngine[Any], nulls: NullPolicy
    ) -> Interpretation:
        arr = as_array(arr)
        if not is_text_like(arr.type):
            return self._interpret_typed(arr, sa_type)
        if self.distinct:
            encoded = arr if pa.types.is_dictionary(arr.type) else pc.dictionary_encode(arr)
            values, invalid = self._interpret_text(encoded.dictionary, sa_type, nulls)
            invalid = pc.fill_null(pc.take(invalid, encoded.indices), False)
            return Interpretation(pc.take(values, encoded.indices), invalid)
        if pa.types.is_dictionary(arr.type):
            arr = arr.dictionary_decode()
        return self._interpret_text(arr, sa_type, nulls)

    def _interpret_text(self, raw: pa.Array, sa_type: TypeEngine[Any], nulls: NullPolicy) -> Interpretation:
        trimmed = pc.utf8_trim_whitespace(raw)
        missing = self.missing(trimmed, nulls)
        cleaned = pc.if_else(missing, NULL_STRING, self.clean(raw, trimmed))
        values, invalid = self.parse(cleaned, sa_type)
        rejected = self.check(values, sa_type)
        return Interpretation(values, invalid if rejected is None else pc.or_(invalid, rejected))

    def _interpret_typed(self, arr: pa.Array, sa_type: TypeEngine[Any]) -> Interpretation:
        target = self.arrow_type(sa_type)
        if pa.types.is_null(arr.type):
            return Interpretation(pa.nulls(len(arr), target), no_failures(len(arr)))
        if pa.types.is_floating(arr.type):
            arr = pc.if_else(pc.fill_null(pc.is_nan(arr), False), pa.scalar(None, arr.type), arr)
        values, invalid = safe_cast(arr, target)
        round_trip, _ = safe_cast(values, arr.type)
        invalid = pc.or_(invalid, pc.fill_null(pc.not_equal(round_trip, arr), False))
        rejected = self.check(values, sa_type)
        return Interpretation(values, invalid if rejected is None else pc.or_(invalid, rejected))
