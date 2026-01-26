
from typing import Any, Type, List, Dict, TYPE_CHECKING,  Iterable, cast
from dataclasses import dataclass, field
import sqlalchemy as sa
import sqlalchemy.orm as so
from pathlib import Path
import pandas as pd
import pyarrow as pa
from logging import getLogger

logger = getLogger(__name__)

if TYPE_CHECKING:
    from ..tables.typing import CSVTableProtocol


"""
Loader Data Structures
======================

This module defines shared data structures and base interfaces used by
file loaders.

It includes:
- the LoaderContext coordination object
- the abstract LoaderInterface
- casting statistics helpers for diagnostics and logging

No file I/O or database-specific loading logic is implemented here.
"""


@dataclass(frozen=True)
class LoaderContext:

    """
    Immutable context object passed through loader operations.

    The LoaderContext encapsulates all state required to load a file
    into a staging table, without relying on global variables.

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
        Optional chunk size for incremental loading.
    normalise
        Whether to apply type casting / normalisation.
    dedupe
        Whether to perform deduplication.
    dedupe_incl_db
        Whether deduplication should include existing database rows.
    """
    tableclass: Type["CSVTableProtocol"]
    session: so.Session
    path: Path
    staging_table: sa.Table

    chunksize: int | None = None
    normalise: bool = True
    dedupe: bool = True
    dedupe_incl_db: bool = False

class LoaderInterface:

    """
    Abstract interface for file loaders.

    Loader implementations are responsible for:
    - reading input files
    - optional chunking
    - optional deduplication
    - optional normalisation
    - loading into a staging table

    Concrete loaders must implement ``orm_file_load`` and ``dedupe``.
    """

    @classmethod
    def orm_file_load(cls, ctx: LoaderContext) -> int:
        """
        Load a file into the staging table.

        Parameters
        ----------
        ctx
            Loader context.

        Returns
        -------
        int
            Number of rows loaded.
        """
        raise NotImplementedError

    @classmethod
    def _load_chunk(
        cls,
        staging_cls: sa.Table,
        session: so.Session,
        dataframe: pd.DataFrame
    ) -> int:
        """
        Load a single DataFrame chunk into the staging table.

        Parameters
        ----------
        staging_cls
            SQLAlchemy Table object representing the staging table.
        session
            Active SQLAlchemy session.
        dataframe
            DataFrame containing rows to insert.

        Returns
        -------
        int
            Number of rows inserted.
        """
        if dataframe.empty:
            return 0
        # convert NaN to None for proper NULL insertion
        dataframe = dataframe.where(pd.notna(dataframe), None) # type: ignore
        records = cast(
            Iterable[Dict[str, Any]],
            dataframe.to_dict(orient="records"),
        )
        
        session.execute( 
            staging_cls.insert(),
            records, # type: ignore
        )
        session.flush()
        session.expunge_all()

        return len(dataframe)
    
    @classmethod
    def dedupe(cls, data: pd.DataFrame | pa.Table, ctx: LoaderContext) -> Any:
        """
        Deduplicate incoming data.

        Concrete implementations define deduplication semantics.

        Parameters
        ----------
        data
            Input data.
        ctx
            Loader context.

        Returns
        -------
        Any
            Deduplicated data.
        """
        raise NotImplementedError
    
    @classmethod
    def _dedupe_db(cls, df: pd.DataFrame, ctx: LoaderContext) -> pd.DataFrame:
        """
        Perform database-level deduplication against existing rows.

        Parameters
        ----------
        df
            Incoming DataFrame.
        ctx
            Loader context.

        Returns
        -------
        pandas.DataFrame
            DataFrame with rows already present in the database removed.
        """
        pk_names = ctx.tableclass.pk_names()
        pk_tuples = list(df[pk_names].itertuples(index=False, name=None))
        if not pk_tuples:
            return df
        tableclass = ctx.staging_table or ctx.tableclass.__table__
        pk_cols = [getattr(tableclass.c, pk) for pk in pk_names]

        vars_per_row = len(pk_cols)
        chunk_size = max(1, 10_000 // vars_per_row)
        existing_rows: list[tuple] = []

        for i in range(0, len(pk_tuples), chunk_size):
            chunk = pk_tuples[i : i + chunk_size]

            rows = (
                ctx.session.query(*pk_cols)
                .filter(sa.tuple_(*pk_cols).in_(chunk))
                .all()
            )
            existing_rows.extend(rows)

        if not existing_rows:
            return df

        existing = pd.DataFrame(existing_rows, columns=pk_names)

        logger.warning(f"Dropping {len(existing)} rows from {ctx.tableclass.__tablename__} that already exist in the database")
        df = (
            df.merge(existing, on=pk_names, how="left", indicator=True)
            .loc[lambda x: x["_merge"] == "left_only"]
            .drop(columns="_merge")
        )
        return df


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
        """
        Record a casting failure for a column.
        """
        if column not in self.columns:
            self.columns[column] = ColumnCastingStats()
        self.columns[column].record(value, example_limit=example_limit)

    @property
    def total_failures(self) -> int:
        """
        Total number of casting failures.
        """
        return sum(stats.count for stats in self.columns.values())

    def has_failures(self) -> bool:
        """
        Whether any casting failures occurred.
        """
        return self.total_failures > 0
    
    def to_dict(self) -> dict[str, dict[str, Any]]:
        """
        Return a dictionary representation of casting statistics.
        """
        return {
            col: {
                "count": stats.count,
                "examples": stats.examples,
            }
            for col, stats in self.columns.items()
        }
    

