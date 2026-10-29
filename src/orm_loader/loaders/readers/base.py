"""The contract every input format fulfils to feed a load."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import sqlalchemy as sa

from ...conversion import NullPolicy


class FileReader(ABC):
    """A file read as Arrow record batches of the model's columns.

    A reader yields batches holding only the model columns present in the
    file, named as the model names them. Text formats yield raw strings, so
    the null policy sees exactly what the file contains. Typed formats yield
    typed columns. Markers that are quote-sensitive are applied while
    reading, because quoting is not visible afterwards.

    Parameters
    ----------
    path
        File to read.
    model_columns
        Model columns by name; only these are read.
    nulls
        Null policy whose quote-sensitive markers are applied while reading.

    Attributes
    ----------
    columns
        Model columns present in the file, in file order.
    empty
        True when the file has no content at all.
    """

    def __init__(
        self,
        path: Path,
        model_columns: Mapping[str, sa.ColumnElement[Any]],
        nulls: NullPolicy | None = None,
    ) -> None:
        self.path = path
        self.model_columns = model_columns
        self.nulls = nulls or NullPolicy()
        self.columns: list[str] = []
        self.empty = False

    @abstractmethod
    def batches(self) -> Iterator[pa.RecordBatch]:
        """Yield the file's rows as non-empty record batches."""

    def reader_markers(self) -> frozenset[str]:
        """Return the markers that must be applied while reading."""
        if self.nulls.marker_quoting == "unquoted_only":
            return self.nulls.text_markers
        return frozenset()

    @staticmethod
    def null_markers(batch: pa.RecordBatch, markers: frozenset[str]) -> pa.RecordBatch:
        """Set string values equal to a marker, ignoring padding, to null."""
        values = pa.array(sorted(markers), pa.string())
        arrays = []
        for column in batch.columns:
            if pa.types.is_string(column.type):
                hit = pc.fill_null(pc.is_in(pc.utf8_trim_whitespace(column), value_set=values), False)
                column = pc.if_else(hit, pa.scalar(None, pa.string()), column)
            arrays.append(column)
        return pa.RecordBatch.from_arrays(arrays, names=batch.schema.names)
