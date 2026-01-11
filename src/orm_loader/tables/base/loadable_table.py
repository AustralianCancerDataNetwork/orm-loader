from .orm_table import ORMTableBase
import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Type
import pandas as pd
from pathlib import Path
import logging

from .typing import CSVTableProtocol
from ..data.ingestion import cast_dataframe_to_model, load_file

logger = logging.getLogger(__name__)

class CSVLoadableTableInterface(ORMTableBase):
    """
    Mixin for ORM tables that can be loaded from CSV files.
    """

    __abstract__ = True

    @classmethod
    def load_csv(
        cls: Type[CSVTableProtocol],
        session: so.Session,
        path: Path,
        *,
        delimiter: str = "\t",
        normalise: bool = True,
        dedupe: bool = False,
        chunksize: int | None = None,
        commit_on_chunk: bool = False,
        dedupe_incl_db: bool = False,
    ) -> int:
        logger.debug("Loading CSV for %s", cls.__tablename__)

        if path.stem.lower() != cls.__tablename__:
            raise ValueError(
                f"CSV filename '{path.name}' does not match table '{cls.__tablename__}'"
            )
        
        total = load_file(
            cls=cls,
            session=session,
            path=path,
            delimiter=delimiter,
            chunksize=chunksize,
            commit_per_chunk=commit_on_chunk,
            normalise=normalise,
            dedupe=dedupe,
            dedupe_incl_db=dedupe_incl_db
        )
        
        return total

    @classmethod
    def csv_columns(cls) -> dict[str, sa.ColumnElement]:
        """
        Return a mapping of CSV column names to SQLAlchemy columns.
        By default, this is the same as model_columns().
        Override this method to provide custom mappings.
        """
        return cls.model_columns()
    
    @classmethod
    def dedupe_dataframe(
        cls: Type[CSVTableProtocol],
        df: pd.DataFrame,
        *,
        session: so.Session | None = None,
        max_bind_vars: int = 10_000,
    ) -> pd.DataFrame:
        """
        Remove rows that already exist in the database or are duplicated
        within the incoming dataframe, based on primary keys. 

        If `session` is None, only internal duplicates are removed.
        """
        if df.empty:
            return df
        pk_names = cls.pk_names()

        before = len(df)
        df = df.drop_duplicates(subset=pk_names, keep="first")
        dropped_internal = before - len(df)
        if dropped_internal > 0:
            logger.info(
                "Dropped %d duplicate rows within chunk for %s",
                dropped_internal,
                cls.__tablename__,
            )

        if session is None:
            return df
        
        pk_tuples = list(df[pk_names].itertuples(index=False, name=None))
        if not pk_tuples:
            return df

        pk_cols = [getattr(cls, c) for c in pk_names]

        vars_per_row = len(pk_cols)
        chunk_size = max_bind_vars // vars_per_row
        if chunk_size <= 0:
            raise ValueError(f"max_bind_vars ({max_bind_vars}) too small for PK size {vars_per_row}")

        existing_rows: list[tuple] = []

        for i in range(0, len(pk_tuples), chunk_size):
            chunk = pk_tuples[i : i + chunk_size]

            rows = (
                session.query(*pk_cols)
                .filter(sa.tuple_(*pk_cols).in_(chunk))
                .all()
            )
            existing_rows.extend(rows)

        if not existing_rows:
            return df

        existing = pd.DataFrame(existing_rows, columns=pk_names)

        logger.warning(
            "Dropping %d rows from %s that already exist in the database",
            len(existing),
            cls.__tablename__,
        )
        df = (
            df.merge(existing, on=pk_names, how="left", indicator=True)
            .loc[lambda x: x["_merge"] == "left_only"]
            .drop(columns="_merge")
        )
        return df
    
    @classmethod
    def normalise_dataframe(cls: Type[CSVTableProtocol], df: pd.DataFrame) -> pd.DataFrame:
        return cast_dataframe_to_model(
            df=df,
            model_columns=cls.model_columns(),
            table_name=cls.__tablename__,
        )


class ParquetLoadableTableMixin(ORMTableBase):
    """
    Mixin for ORM tables that can be loaded from Parquet files.
    """

    __abstract__ = True

    @classmethod
    def load_parquet(
        cls,
        session: so.Session,
        path: Path,
        *,
        columns: list[str] | None = None,
        filters: list[tuple] | None = None,
        commit_per_chunk: bool = False,
    ) -> int:
        raise NotImplementedError("Parquet loading not implemented for this table")

