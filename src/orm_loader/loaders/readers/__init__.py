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
    quote_mode: str = "by_delimiter",
    chunksize: int | None = None,
    skip_malformed: bool = False,
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
    skip_malformed
        Leave out lines with the wrong number of fields and record them in
        the reader's ``skipped`` list. Delimited text only.

    Returns
    -------
    FileReader
        The opened reader.
    """
    if path.suffix.lower() == ".parquet":
        return ParquetReader(path, model_columns, nulls, chunksize)
    return DelimitedTextReader(path, model_columns, nulls, quote_mode, skip_malformed=skip_malformed)




__all__ = ["DelimitedTextReader", "FileReader", "ParquetReader", "open_reader"]
