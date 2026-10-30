"""Parquet files as record batches."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds
import sqlalchemy as sa

from ...conversion import NullPolicy
from .base import FileReader


class ParquetReader(FileReader):
    """A Parquet file read with its stored types.

    Column names are normalised as described by ``normalise_headers``.

    Parameters
    ----------
    path
        File to read.
    model_columns
        Model columns by name; only these are read.
    nulls
        Null policy whose quote-sensitive markers are applied while reading. A
        Parquet string was never quoted, so every marker match counts as
        unquoted.
    chunksize
        Rows per batch.
    """

    def __init__(
        self,
        path: Path,
        model_columns: Mapping[str, sa.ColumnElement[Any]],
        nulls: NullPolicy | None = None,
        chunksize: int | None = None,
    ) -> None:
        super().__init__(path, model_columns, nulls)
        self.chunksize = chunksize or 64_000
        self._dataset = ds.dataset(path, format="parquet")
        self.file_columns = self.normalise_headers(self._dataset.schema.names)
        stored = dict(zip(self.file_columns, self._dataset.schema.names))
        self.columns = [name for name in self.file_columns if name in model_columns]
        self._stored = [stored[name] for name in self.columns]

    def batches(self) -> Iterator[pa.RecordBatch]:
        """Yield the stored rows in batches of ``chunksize`` rows."""
        markers = self.reader_markers()
        for batch in self._dataset.to_batches(columns=self._stored, batch_size=self.chunksize):
            batch = batch.rename_columns(self.columns)
            if markers:
                batch = self.null_markers(batch, markers)
            if batch.num_rows:
                yield batch
