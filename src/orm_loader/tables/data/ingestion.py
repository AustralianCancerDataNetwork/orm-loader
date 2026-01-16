from dataclasses import dataclass, field
from typing import Any, List, Dict, Iterable, Type, cast
import logging
from pathlib import Path
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.orm as so
import chardet
from ..base.typing import CSVTableProtocol
from .data_type_management import _safe_cast

logger = logging.getLogger(__name__)

@dataclass
class ColumnCastingStats:
    """
    Casting statistics for a single column.
    """
    count: int = 0
    examples: List[Any] = field(default_factory=list)

    def record(self, value: Any, example_limit: int = 3):
        self.count += 1
        if len(self.examples) < example_limit:
            self.examples.append(value)

@dataclass
class TableCastingStats:
    """
    Aggregated casting statistics for a table, keyed by column.
    """
    table_name: str
    columns: Dict[str, ColumnCastingStats] = field(default_factory=dict)

    def record(
        self,
        *,
        column: str,
        value: Any,
        example_limit: int = 3,
    ):
        if column not in self.columns:
            self.columns[column] = ColumnCastingStats()
        self.columns[column].record(value, example_limit=example_limit)

    @property
    def total_failures(self) -> int:
        return sum(stats.count for stats in self.columns.values())

    def has_failures(self) -> bool:
        return self.total_failures > 0
    
    def to_dict(self) -> dict[str, dict[str, Any]]:
        return {
            col: {
                "count": stats.count,
                "examples": stats.examples,
            }
            for col, stats in self.columns.items()
        }
    
def infer_encoding(file):
    with open(file, 'rb') as infile:
        encoding = chardet.detect(infile.read(10000))
    if encoding['encoding'] == 'ascii':
        encoding['encoding'] = 'utf-8' # utf-8 valid superset of ascii, so being more conservative here just because it flakes occasionally
    return encoding

def infer_delim(file):
    with open(file, 'r') as infile:
        line = infile.readline()
        tabs = line.count('\t')
        commas = line.count(',')
        if tabs > commas:
            return '\t'
        return ','
    
def cast_dataframe_to_model(
    *,
    df: pd.DataFrame,
    model_columns: dict[str, sa.ColumnElement],
    table_name: str,
) -> pd.DataFrame:
    
    """
    Cast DataFrame columns to SQLAlchemy model column types.

    - Applies per-column type casting
    - Drops rows violating required (non-nullable, no-default) constraints
    - Emits warnings with example values for cast failures

    Policy decisions (whether to call this, how strict to be)
    are handled by the table mixin.
    """
    if df.empty:
        return df

    stats = TableCastingStats(table_name=table_name)

    for col_name, sa_col in model_columns.items():
        if col_name not in df.columns:
            continue

        def _on_cast_error(value, *, _col=col_name):
            stats.record(column=_col, value=value)

        df[col_name] = df[col_name].map(
            lambda v: _safe_cast(v, sa_col.type, on_error=_on_cast_error)
        )

    required_cols = [
        name
        for name, col in model_columns.items()
        if not col.nullable and not col.default and not col.server_default
    ]

    if required_cols:
        null_mask = df[required_cols].isna()
        for col in required_cols:
            null_count = int(null_mask[col].sum())
            if null_count > 0:
                logger.warning(
                    "Found %d rows with unexpected nulls in %s.%s",
                    null_count,
                    table_name,
                    col,
                )

        # Drop rows violating required constraints
        df = df.loc[~null_mask.any(axis=1)]

    if stats.has_failures():
        for col, col_stats in stats.columns.items():
            logger.warning(f"CAST {table_name}.{col}: {col_stats.count} row(s) failed. Examples: {col_stats.examples}")

    return df


def load_chunk(
    *,
    cls,
    session: so.Session,
    dataframe: pd.DataFrame,
    commit: bool = False,
    table_override: sa.Table | None = None,
) -> int:
    """Load a chunk of data into the given ORM table class."""
    if dataframe.empty:
        return 0
    records = cast(
        Iterable[Dict[str, Any]],
        dataframe.to_dict(orient="records"),
    )
    
    # this is for when we are inserting into staging tables instead 
    # of the ORM-mapped table to support upsert/merge workflows
    if table_override is not None:
        session.execute( 
            table_override.insert(),
            records, # type: ignore
        )
    else:
        session.bulk_insert_mappings(cls, records)
    if commit:
        logger.debug(f"Committing chunk of {len(dataframe)} rows to {cls.__tablename__}")
        session.commit()

    return len(dataframe)

def load_file(
    *,
    cls: Type[CSVTableProtocol],
    session: so.Session,
    path: Path,
    dtype=str,
    chunksize: int | None = None,
    commit_on_chunk: bool = False,
    normalise: bool = True,
    dedupe: bool = True,
    dedupe_incl_db: bool = False,
    table_override: sa.Table | None = None,
) -> int:
    """
    Load a file into a table, delegating chunking to pandas.

    If chunksize is None, pandas returns a single DataFrame, which we
    normalise to a one-element iterator for unified processing.
    """
    total = 0

    delimiter = infer_delim(path)
    encoding = infer_encoding(path)['encoding']

    try:
        reader = pd.read_csv(
            path,
            delimiter=delimiter,
            dtype=dtype,
            chunksize=chunksize,
            encoding=encoding,
        )
    except pd.errors.EmptyDataError:
        logger.info(f"File {path.name} is empty — skipping load for {cls.__tablename__}")
        return 0
    
    logger.info(f"Detected encoding {encoding} for file {path.name}")
    logger.info(f"Detected delimiter '{delimiter}' for file {path.name}")

    chunks = (reader,) if isinstance(reader, pd.DataFrame) else reader

    for chunk in chunks:
        if normalise:
            chunk = cls.normalise_dataframe(chunk)
        if dedupe:
            chunk = cls.dedupe_dataframe(chunk, session=session if dedupe_incl_db else None)
        total += load_chunk(
            cls=cls,
            session=session,
            dataframe=chunk,
            commit=commit_on_chunk,
            table_override=table_override,
        )

    return total