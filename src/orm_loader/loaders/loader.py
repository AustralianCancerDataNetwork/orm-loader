"""File loading
============

A reader yields a file as Arrow batches, the batch processor interprets them
under the load's conversion policy and the backend's sink writes them to the
staging table. Every backend and file type takes the same path.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from itertools import chain
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Type

import sqlalchemy as sa
import sqlalchemy.orm as so

from ..backends.base import DedupeResult
from ..backends.resolve import resolve_backend
from ..conversion import ConversionPolicy
from ..helpers import IngestError
from .processor import BatchProcessor
from .readers import FileReader

if TYPE_CHECKING:
    from ..tables.typing import FileTableProtocol

logger = logging.getLogger(__name__)


@dataclass
class LoadReport:
    """Row accounting for one load.

    Attributes
    ----------
    rows_read
        Data rows read from the file.
    rows_loaded
        Rows written to the staging table.
    rows_rejected
        Rows dropped because a value was unreadable or a required value was
        missing, with ``ErrorPolicy.on_error`` set to ``"reject_row"``.
    dedupe
        Result of deduplicating the staging table, if it was requested.
    rejects_path
        File the rejected rows were written to, if any.
    """

    rows_read: int = 0
    rows_loaded: int = 0
    rows_rejected: int = 0
    dedupe: DedupeResult | None = None
    rejects_path: Path | None = None

    @property
    def rows_deduplicated(self) -> int:
        """Rows removed as duplicates, zero when deduplication did not run."""
        return self.dedupe.rows_removed if self.dedupe else 0

    @property
    def accounted(self) -> bool:
        """Whether every row read was loaded, rejected or dropped as a duplicate."""
        return self.rows_read == self.rows_loaded + self.rows_rejected + self.rows_deduplicated


@dataclass(frozen=True)
class LoaderContext:
    """Immutable context passed through a load.

    The context holds all state required to load a file into a staging
    table, without relying on global variables.

    Attributes
    ----------
    tableclass
        ORM table class being loaded.
    session
        Active SQLAlchemy session.
    path
        Path to the source data file.
    staging_table
        SQLAlchemy Table object representing the staging table.
    chunksize
        Rows per batch for the default reader of formats that batch by rows.
    normalise
        Whether to interpret values under the policy.
    dedupe
        Whether the staging table keeps only the first row for each primary key.
    quote_mode
        Quoting mode of the default reader for delimited text: ``"csv"``,
        ``"literal"``, ``"by_delimiter"`` or ``"auto"``.
    staging_schema_tag
        schema_translate_map tag the staging table lives in, passed to
        resolve_backend() so every backend resolution within this load
        resolves the same schema.
    policy
        How values are interpreted and what happens to unreadable ones.
    rejects_dir
        Directory that rejected rows are written to. Defaults to a
        ``rejects`` directory inside the source file's directory.
    report
        Row accounting, filled in as the load runs.
    """

    tableclass: Type["FileTableProtocol"]
    session: so.Session
    path: Path
    staging_table: sa.Table

    chunksize: int | None = None
    normalise: bool = True
    dedupe: bool = True
    quote_mode: str = "auto"
    staging_schema_tag: str | None = None
    policy: ConversionPolicy = field(default_factory=ConversionPolicy)
    rejects_dir: Path | None = None
    report: LoadReport = field(default_factory=LoadReport)


def _require_columns_present(tableclass: Type["FileTableProtocol"], present_columns: Iterable[str]) -> None:
    """Raise before staging if a required column is entirely absent from the file.

    Partial-column loads are not supported, so every merge strategy
    (including upsert) needs a complete row shape and a missing required
    column is always a data problem.

    Parameters
    ----------
    tableclass
        Table being loaded.
    present_columns
        Model columns present in the file.

    Raises
    ------
    IngestError
        A non-nullable column without a default is not in the file.
    """
    present = set(present_columns)
    missing = sorted(c for c in tableclass.required_columns() if c not in present)
    if missing:
        raise IngestError(
            f"{tableclass.__tablename__}: source data is missing required column(s) {missing} (non-nullable, no default)"
        )


def stage_file(ctx: LoaderContext, reader: FileReader) -> int:
    """Load a file into the staging table.

    The reader streams the file in bounded batches, so memory does not grow
    with the row count. A file without data rows loads nothing and is not
    checked for required columns. With ``ctx.dedupe`` set, the staging table keeps the
    first row for each primary key.

    Parameters
    ----------
    ctx
        Loader context.
    reader
        Reader that yields the file as Arrow batches.

    Returns
    -------
    int
        Number of rows left in the staging table.

    Raises
    ------
    IngestError
        The file cannot be read, is missing a required column, or the rows
        read are not all accounted for.
    ConversionError
        A value cannot be read as its column type.
    RequiredValueError
        A required column has no value.
    RejectLimitError
        Rejected rows exceed ``ErrorPolicy.max_rejects``.
    """
    table_name = ctx.tableclass.__tablename__
    batches = reader.batches()
    first = None if reader.empty else next(batches, None)
    if first is None:
        logger.info("File %s has no data rows; skipping load for %s", ctx.path.name, table_name)
        return 0
    if ctx.normalise:
        _require_columns_present(ctx.tableclass, reader.columns)
    rejects_dir = ctx.rejects_dir or ctx.path.parent / "rejects"
    processor = BatchProcessor(
        ctx.tableclass,
        reader.columns,
        ctx.policy,
        ctx.report,
        rejects_dir / f"{ctx.path.stem}.rejects.csv",
        ctx.normalise,
    )
    backend = resolve_backend(ctx.session, staging_schema_tag=ctx.staging_schema_tag, mapper=ctx.tableclass)
    total = backend.insert_batches(ctx, processor.stream(chain([first], batches)), reader.columns)
    keys = [c for c in ctx.tableclass.pk_names() if c in reader.columns]
    if ctx.dedupe and keys:
        ctx.report.dedupe = backend.deduplicate_staging(ctx, keys)
        total = ctx.report.dedupe.rows_after
    ctx.report.rows_loaded += total
    if not ctx.report.accounted:
        raise IngestError(
            f"{table_name}: read {ctx.report.rows_read} rows but loaded {ctx.report.rows_loaded}, "
            f"rejected {ctx.report.rows_rejected} and dropped {ctx.report.rows_deduplicated} duplicates"
        )
    return total
