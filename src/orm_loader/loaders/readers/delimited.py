"""CSV and other delimited text files as record batches of raw text."""

from __future__ import annotations

import codecs
import logging
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pyarrow.csv as pv
import sqlalchemy as sa

from ...conversion import NullPolicy
from ...helpers import IngestError
from .base import FileReader
from .detection import infer_delim, infer_encoding, resolve_quote_mode

logger = logging.getLogger(__name__)

QUOTE_SAMPLE_LINES = 50


class DelimitedTextReader(FileReader):
    """A delimited text file read as raw strings in bounded blocks.

    Date columns are dictionary-encoded. An unquoted empty field is null and
    a quoted empty field is an empty string. Header names are normalised as
    described by ``normalise_headers``. File columns that are not model
    columns are ignored.

    Parameters
    ----------
    path
        File to read.
    model_columns
        Model columns by name; only these are read.
    nulls
        Null policy whose quote-sensitive markers are applied while reading.
    quote_mode
        ``"csv"`` reads double quotes as field quoting, ``"literal"`` keeps
        them as data, ``"by_delimiter"`` chooses ``"literal"`` for tab
        delimited files and ``"csv"`` otherwise.
    block_size
        Bytes per block; memory use follows this, not the row count.
    skip_malformed
        Leave out a line with the wrong number of fields and record it in
        ``skipped``, instead of failing the read.
    """

    def __init__(
        self,
        path: Path,
        model_columns: Mapping[str, sa.ColumnElement[Any]],
        nulls: NullPolicy | None = None,
        quote_mode: str = "by_delimiter",
        block_size: int = 4 << 20,
        skip_malformed: bool = False,
    ) -> None:
        super().__init__(path, model_columns, nulls)
        self.block_size = block_size
        self.skip_malformed = skip_malformed
        if path.stat().st_size == 0:
            self.empty = True
            return
        self.delimiter = infer_delim(path)
        self.encoding = infer_encoding(path)["encoding"] or "utf-8"
        try:
            codecs.lookup(self.encoding)
        except LookupError as exc:
            raise IngestError(f"{path.name}: unsupported encoding {self.encoding!r}") from exc
        self.quote = resolve_quote_mode(quote_mode, self.delimiter)
        self.file_columns = self._header_names()
        self.columns = [name for name in self.file_columns if name in model_columns]
        if self.nulls.marker_quoting == "unquoted_only" and self.nulls.text_markers:
            self._warn_if_everything_is_quoted()

    def _header_names(self) -> list[str]:
        with open(self.path, "rb") as handle:
            header = handle.readline().decode(self.encoding, errors="replace")
        return self.normalise_headers(header.lstrip("\ufeff").rstrip("\r\n").split(self.delimiter))

    def _skip_row(self, row: pv.InvalidRow) -> Literal["skip"]:
        self.skipped.append((row.number, f"expected {row.expected_columns} columns, got {row.actual_columns}: {row.text.rstrip()}"))
        return "skip"

    def _warn_if_everything_is_quoted(self) -> None:
        with open(self.path, "rb") as handle:
            handle.readline()
            lines = [handle.readline().decode(self.encoding, errors="replace") for _ in range(QUOTE_SAMPLE_LINES)]
        fields = [f for line in lines if line.strip() for f in line.rstrip("\r\n").split(self.delimiter)]
        if fields and all(f.startswith('"') for f in fields):
            logger.warning(
                "%s quotes every field, so quote-sensitive text markers %s can never match",
                self.path.name,
                sorted(self.nulls.text_markers),
            )

    def batches(self) -> Iterator[pa.RecordBatch]:
        """Yield the file's rows in blocks of ``block_size`` bytes."""
        if self.empty:
            return
        string, encoded = pa.string(), pa.dictionary(pa.int32(), pa.string())
        column_types = {
            name: encoded if isinstance(self.model_columns[name].type, sa.Date) else string
            for name in self.columns
        }
        try:
            reader = pv.open_csv(
                self.path,
                read_options=pv.ReadOptions(
                    column_names=self.file_columns,
                    skip_rows=1,
                    block_size=self.block_size,
                    encoding=self.encoding,
                    use_threads=not self.skip_malformed,
                ),
                parse_options=pv.ParseOptions(
                    delimiter=self.delimiter,
                    quote_char='"' if self.quote == "csv" else False,
                    escape_char=False,
                    newlines_in_values=self.quote == "csv",
                    invalid_row_handler=self._skip_row if self.skip_malformed else None,
                ),
                convert_options=pv.ConvertOptions(
                    column_types=column_types,
                    include_columns=self.columns,
                    null_values=[""] + sorted(self.reader_markers()),
                    strings_can_be_null=True,
                    quoted_strings_can_be_null=False,
                ),
            )
            with reader:
                for batch in reader:
                    if batch.num_rows:
                        yield batch
        except UnicodeDecodeError as exc:
            raise IngestError(f"{self.path.name}: cannot decode the file as {self.encoding}: {exc.reason}.") from exc
        except pa.ArrowInvalid as exc:
            if "Empty CSV file" in str(exc):
                return
            hint = (
                " The file may contain literal double quotes; try quote_mode='literal' or 'by_delimiter'."
                if self.quote == "csv" and str(exc).startswith("CSV parse error")
                else ""
            )
            raise IngestError(f"{self.path.name}: {exc}.{hint}") from exc
