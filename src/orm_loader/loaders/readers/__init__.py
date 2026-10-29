from collections.abc import Mapping
from pathlib import Path
from typing import Any

import sqlalchemy as sa

from ...conversion import NullPolicy
from .base import FileReader
from .delimited import DelimitedTextReader
from .parquet import ParquetReader


def open_reader(
    path: Path,
    model_columns: Mapping[str, sa.ColumnElement[Any]],
    *,
    nulls: NullPolicy | None = None,
    quote_mode: str = "auto",
    chunksize: int | None = None,
) -> FileReader:
    """Open a file with the reader for its format.

    A ``.parquet`` suffix selects Parquet; every other suffix is read as
    delimited text.

    Parameters
    ----------
    path
        File to read.
    model_columns
        Model columns by name; only these are read.
    nulls
        Null policy of the load.
    quote_mode
        Quote handling for delimited text.
    chunksize
        Rows per batch for formats that batch by rows.

    Returns
    -------
    FileReader
        The opened reader.
    """
    if path.suffix.lower() == ".parquet":
        return ParquetReader(path, model_columns, nulls, chunksize)
    return DelimitedTextReader(path, model_columns, nulls, quote_mode)




__all__ = ["DelimitedTextReader", "FileReader", "ParquetReader", "open_reader"]
