"""Interpretation of raw batches into typed rows ready for a staging sink."""

from __future__ import annotations

import logging
import os
from bisect import bisect_left
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
import sqlalchemy as sa

from ..conversion import ConversionPolicy, Interpretation
from ..conversion.values import no_failures, underlying_type
from ..helpers import (
    ConversionError,
    IngestError,
    PolicyError,
    RejectLimitError,
    RequiredValueError,
)

if TYPE_CHECKING:
    from ..tables.typing import FileTableProtocol
    from .loader import LoadReport

logger = logging.getLogger(__name__)


class RejectsWriter:
    """Writes rejected rows, with their raw text and why they were rejected, to a CSV file.

    The file has the columns ``row`` (data row number starting at 1),
    ``column`` (first column that failed), ``reason`` and then every column
    read from the source as raw text.

    Parameters
    ----------
    path
        File to write.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._writer: pv.CSVWriter | None = None

    def check_writable(self) -> None:
        """Raise IngestError unless the file can be created.

        Raises
        ------
        IngestError
            The directory, or the nearest existing parent of a directory that
            does not exist yet, is not writable.
        """
        directory = self.path.parent
        while not directory.exists() and directory != directory.parent:
            directory = directory.parent
        if not directory.is_dir() or not os.access(directory, os.W_OK):
            raise IngestError(f"Cannot write rejected rows to {self.path.parent}: directory is not writable")

    def write(self, rows: pa.Table) -> None:
        """Append rejected rows, creating the directory and file on first use."""
        if self._writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pv.CSVWriter(str(self.path), rows.schema)
        self._writer.write_table(rows)

    def close(self) -> None:
        """Close the file if it was opened."""
        if self._writer is not None:
            self._writer.close()
            self._writer = None


class BatchProcessor:
    """Turn raw batches into typed batches under the load's conversion policy.

    A batch with an unreadable value or a missing required value either
    raises or has those rows written to the rejects file, depending on
    ``ErrorPolicy.on_error``. A rejects file left by an earlier load is
    removed.

    Parameters
    ----------
    tableclass
        Table the rows are loaded into.
    columns
        Model columns present in the reader.
    policy
        How values are interpreted and what happens to unreadable ones.
    report
        Row accounting, updated as batches are processed.
    rejects_path
        File that rejected rows are written to.
    normalise
        Whether to interpret values. When false, batches pass through.
    skipped
        Lines the reader left out as malformed, each as its line number in the
        file and the reason. They are written to the rejects file and count as
        rows read and rejected.
    evaluate
        Function that returns the value of a SQL expression default as a
        column of the given type stores it.

    Attributes
    ----------
    out_columns
        Columns of the typed batches: the file's columns, then the columns
        the file lacks that have a default.

    Notes
    -----
    A missing value takes the column's default, and a column the file lacks
    is filled with it. The default is read by the column's handler like a
    value from the file. A SQL expression default is evaluated for each batch
    that needs it. A missing value left in a non-nullable column is a
    required value that is missing.
    """

    def __init__(
        self,
        tableclass: type[FileTableProtocol],
        columns: list[str],
        policy: ConversionPolicy,
        report: LoadReport,
        rejects_path: Path,
        normalise: bool = True,
        skipped: list[tuple[int | None, str]] | None = None,
        evaluate: Callable[[Any, Any], Any] | None = None,
    ) -> None:
        self.columns = columns
        self.skipped = skipped if skipped is not None else []
        self._parsed = 0
        self._shifts: list[int] = []
        self.policy = policy
        self.report = report
        self.normalise = normalise
        self.table_name = tableclass.__tablename__
        model_columns = tableclass.model_columns()
        self.defaults = tableclass.column_defaults() if normalise else {}
        self.out_columns = [*columns, *(name for name in self.defaults if name not in columns)]
        self.types = {name: underlying_type(model_columns[name].type) for name in self.out_columns}
        self.handlers = (
            {name: policy.for_column(self.types[name], self.table_name, name) for name in self.out_columns}
            if normalise
            else {}
        )
        self.required = [name for name in self.out_columns if not model_columns[name].nullable]
        self.evaluate = evaluate
        self._literals: dict[str, pa.Scalar] = {}
        self.rejects = RejectsWriter(rejects_path)
        if policy.errors.on_error == "reject_row":
            self.rejects.check_writable()
        rejects_path.unlink(missing_ok=True)

    def stream(self, batches: Iterable[pa.RecordBatch]) -> Iterator[pa.Table]:
        """Yield typed tables for the raw batches, closing the rejects file afterwards."""
        try:
            for raw in batches:
                self._reject_skipped()
                table = self.process(raw)
                if table.num_rows:
                    yield table
            self._reject_skipped()
        finally:
            self.rejects.close()

    def process(self, raw: pa.RecordBatch) -> pa.Table:
        """Interpret one raw batch.

        Parameters
        ----------
        raw
            Batch of raw text columns.

        Returns
        -------
        pa.Table
            Typed rows that passed every check.

        Raises
        ------
        ConversionError
            A value cannot be read as its column type and ``on_error`` is
            ``"raise"``.
        RequiredValueError
            A required column has no value and ``on_error`` is ``"raise"``.
        RejectLimitError
            Rejected rows exceed ``max_rejects``.
        """
        first = self._parsed
        self._parsed += raw.num_rows
        self.report.rows_read += raw.num_rows
        if not self.normalise:
            return pa.Table.from_batches([raw])
        results = {
            name: self.handlers[name].interpret(raw.column(name), self.types[name], self.policy.nulls)
            for name in self.columns
        }
        self._fill_defaults(results, raw.num_rows)
        failures = self._failures(results)
        if failures:
            if self.policy.errors.on_error == "raise":
                self._raise(raw, results, first)
            return self._reject(raw, results, failures, first)
        return pa.table({name: results[name].values for name in self.out_columns})

    def _fill_defaults(self, results: dict[str, Interpretation], count: int) -> None:
        """Give a missing value, and a column the file lacks, the column's default."""
        for name, default in self.defaults.items():
            if name not in results:
                results[name] = Interpretation(pa.repeat(self._fill_value(name, default), count), no_failures(count))
                continue
            values, invalid = results[name]
            missing = pc.and_(pc.is_null(values), pc.invert(invalid))
            if pc.any(missing).as_py():
                filled = pc.if_else(missing, self._fill_value(name, default), values)
                results[name] = Interpretation(filled, invalid)

    def _fill_value(self, name: str, default: Any) -> pa.Scalar:
        """Return the scalar that fills a column: a literal is read once, an expression is evaluated for each batch."""
        if isinstance(default, sa.ClauseElement):
            if self.evaluate is None:
                raise PolicyError(table_name=self.table_name, column_name=name, column_type="SQL expression default")
            return self._read_default(name, self.evaluate(default, self.types[name]))
        if name not in self._literals:
            self._literals[name] = self._read_default(name, default)
        return self._literals[name]

    def _read_default(self, name: str, value: Any) -> pa.Scalar:
        """Read a default value the way the column's handler reads a value from the file."""
        values, invalid = self.handlers[name].interpret(pa.array([value]), self.types[name], self.policy.nulls)
        if invalid[0].as_py():
            raise ConversionError(
                table_name=self.table_name,
                column_name=name,
                expected=self.handlers[name].expected(self.types[name]),
                count=1,
                samples=[value],
            )
        return values[0]

    def _failures(self, results: dict[str, Any]) -> dict[str, tuple[pa.Array, str]]:
        found: dict[str, tuple[pa.Array, str]] = {}
        for name in self.columns:
            invalid = results[name].invalid
            if pc.any(invalid).as_py():
                found[name] = (invalid, f"cannot be read as {self.handlers[name].expected(self.types[name])}")
        for name in self.required:
            missing = pc.and_(pc.is_null(results[name].values), pc.invert(results[name].invalid))
            if pc.any(missing).as_py():
                found[name] = (pc.or_(found[name][0], missing) if name in found else missing, "required value is missing")
        return found

    def _conversion_error(self, name: str, raw: pa.Array | pa.ChunkedArray, invalid: pa.Array) -> ConversionError | None:
        """Describe the unreadable values of a column, or return None when there are none.

        Parameters
        ----------
        name
            The column.
        raw
            The raw column the mask refers to.
        invalid
            Boolean mask that is true where a value could not be read.

        Returns
        -------
        ConversionError | None
            An error with the count and first three raw values, or None.
        """
        count = pc.sum(pc.cast(invalid, pa.int64())).as_py() or 0
        if not count:
            return None
        if isinstance(raw, pa.ChunkedArray):
            raw = raw.combine_chunks()
        if pa.types.is_dictionary(raw.type):
            raw = raw.dictionary_decode()
        return ConversionError(
            table_name=self.table_name,
            column_name=name,
            expected=self.handlers[name].expected(self.types[name]),
            count=count,
            samples=raw.filter(invalid).slice(0, 3).to_pylist(),
        )

    def _raise(self, raw: pa.RecordBatch, results: dict[str, Any], first: int) -> None:
        for name in self.columns:
            error = self._conversion_error(name, raw.column(name), results[name].invalid)
            if error is not None:
                raise error
        for name in self.required:
            missing = pc.is_null(results[name].values)
            count = pc.sum(pc.cast(missing, pa.int64())).as_py() or 0
            if count:
                rows = self._row_numbers(first, raw.num_rows).take(pc.indices_nonzero(missing).slice(0, 3)).to_pylist()
                raise RequiredValueError(
                    table_name=self.table_name, column_name=name, count=count, rows=rows
                )

    def _reject(
        self,
        raw: pa.RecordBatch,
        results: dict[str, Any],
        failures: dict[str, tuple[pa.Array, str]],
        first: int,
    ) -> pa.Table:
        rejected = no_failures(raw.num_rows)
        column = pa.nulls(raw.num_rows, pa.string())
        reason = pa.nulls(raw.num_rows, pa.string())
        for name, (mask, why) in failures.items():
            first_hit = pc.and_(mask, pc.invert(rejected))
            column = pc.if_else(first_hit, pa.scalar(name), column)
            reason = pc.if_else(first_hit, pa.scalar(why), reason)
            rejected = pc.or_(rejected, mask)
        row_numbers = self._row_numbers(first, raw.num_rows)
        plain = [c.dictionary_decode() if pa.types.is_dictionary(c.type) else c for c in raw.columns]
        listing = pa.table(
            [row_numbers, column, reason, *plain], names=["row", "column", "reason", *raw.schema.names]
        )
        dropped = listing.filter(rejected)
        self.rejects.write(dropped)
        self.report.rows_rejected += dropped.num_rows
        self.report.rejects_path = self.rejects.path
        self._check_limit()
        keep = pc.invert(rejected)
        return pa.table({n: results[n].values for n in self.out_columns}).filter(keep)

    def _row_numbers(self, first: int, count: int) -> pa.Array:
        """Return the data row numbers in the file of ``count`` parsed rows, counting malformed lines.

        Parameters
        ----------
        first
            Position among the parsed rows of the first row (starting at 0).
        count
            Number of rows.
        """
        positions = range(first + 1, first + count + 1)
        return pa.array([p + bisect_left(self._shifts, p) for p in positions], pa.int64())

    def _reject_skipped(self) -> None:
        """Write the lines the reader left out to the rejects file and remember where they were."""
        count = len(self.skipped)
        if not count:
            return
        text, empty = pa.string(), pa.nulls(count, pa.string())
        rows = pa.table(
            {
                "row": pa.array([None if n is None else n - 1 for n, _ in self.skipped], pa.int64()),
                "column": empty,
                "reason": pa.array([why for _, why in self.skipped], text),
            }
            | {name: empty for name in self.columns}
        )
        for number, _ in self.skipped:
            if number is not None:
                self._shifts.append(number - 1 - (len(self._shifts) + 1))
        self.skipped.clear()
        self.rejects.write(rows)
        self.report.rows_read += count
        self.report.rows_rejected += count
        self.report.rejects_path = self.rejects.path
        self._check_limit()

    def _check_limit(self) -> None:
        limit = self.policy.errors.max_rejects
        if limit is None:
            return
        rejected, read = self.report.rows_rejected, self.report.rows_read
        over = rejected / read > limit if isinstance(limit, float) and limit < 1 else rejected > limit
        if over:
            raise RejectLimitError(rejected=rejected, read=read, limit=limit)
