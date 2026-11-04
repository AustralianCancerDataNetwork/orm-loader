"""The contract every input format fulfils to feed a load."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any

import pyarrow as pa
import sqlalchemy as sa

from ...helpers import IngestError


class FileReader(ABC):
    """A file read as Arrow record batches of the model's columns.

    A reader yields batches holding only the model columns present in the
    file, named as the model names them. Text formats yield raw strings, so
    the null policy sees exactly what the file contains. Typed formats yield
    typed columns. A reader decides nothing about missing values.

    Parameters
    ----------
    path
        File to read.
    model_columns
        Model columns by name; only these are read.

    Attributes
    ----------
    columns
        Model columns present in the file, in file order.
    file_columns
        Every column of the file under its normalised name, in file order.
    empty
        True when the file has no content at all.
    skipped
        Lines the reader left out as malformed, each as the number of the
        line in the file (None when unknown) and the reason, filled in while
        the batches are read.
    """

    def __init__(
        self,
        path: Path,
        model_columns: Mapping[str, sa.ColumnElement[Any]],
    ) -> None:
        self.path = path
        self.model_columns = model_columns
        self.columns: list[str] = []
        self.file_columns: list[str] = []
        self.empty = False
        self.skipped: list[tuple[int | None, str]] = []

    def normalise_headers(self, names: Iterable[str]) -> list[str]:
        """Return header names lowercased, stripped of quotes and padding and of a ``_hash`` suffix.

        Raises
        ------
        IngestError
            Two columns have the same normalised name.
        """
        normalised = [name.strip().strip('"').strip().lower().removesuffix("_hash") for name in names]
        if len(set(normalised)) != len(normalised):
            raise IngestError(f"{self.path.name}: duplicate column names in header {normalised}")
        return normalised

    @abstractmethod
    def batches(self) -> Iterator[pa.RecordBatch]:
        """Yield the file's rows as non-empty record batches."""
