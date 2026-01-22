from __future__ import annotations
from typing import Any
import pandas as pd
import logging
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
from functools import reduce
from .data_classes import LoaderContext, TableCastingStats, LoaderInterface
from .loading_helpers import infer_delim, infer_encoding, conservative_load_parquet, arrow_drop_duplicates
from .data import perform_cast, cast_arrow_column

logger = logging.getLogger(__name__)

"""
File Loader Implementations
===========================

This module provides concrete loader implementations for ingesting
CSV- and Parquet-based datasets into staging tables.

Loaders are intentionally conservative and designed to handle:
- untrusted data sources
- incremental loads
- partial failures
- schema drift

Where supported (PostgreSQL), fast-path COPY-based loading is available
via helper utilities.


These loader interfaces implement a very conservative loading strategy for handling data 
from untrusted sources and accommodating updates and deletes for incremental loads.

"""

class PandasLoader(LoaderInterface):

    """
    For initial dataloads, pandasloader may not be sufficiently performant for very large files.
    However, it provides a very flexible and easy-to-debug pathway for data ingestion, especially
    when dealing with CSV files. It allows for chunked processing, which helps manage memory usage
    effectively. Supports significant transformation or cleaning before being loaded into the database.
    """

    @classmethod
    def dedupe(cls, data: pd.DataFrame | pa.Table, ctx: LoaderContext) -> Any:
        if not isinstance(data, pd.DataFrame):
            df = data.to_pandas()
        else:
            df = data
        if df.empty:
            return df
        pk_names = ctx.tableclass.pk_names()
        before = len(df)
        df = df.drop_duplicates(subset=pk_names, keep='first')
        dropped_internal = before - len(df)
        if dropped_internal > 0:
            logger.info(f"Dropped {dropped_internal} duplicate rows internally in staging for {ctx.tableclass.__tablename__}")        
        if not ctx.dedupe_incl_db:
            return df
        else:
            logger.info(f"Performing DB-level deduplication for {ctx.tableclass.__tablename__}")
            # DB-level dedupe
            return cls._dedupe_db(df, ctx)

    @classmethod
    def cast_to_model(cls, data: pd.DataFrame | pa.Table, ctx: LoaderContext) -> Any:
        if not isinstance(data, pd.DataFrame):
            df = data.to_pandas()
        else:
            df = data
        if df.empty:
            return df
        
        table_name = ctx.tableclass.__tablename__
        stats = TableCastingStats(table_name=table_name)

        model_columns = ctx.tableclass.model_columns()
        for col_name, sa_col in model_columns.items():
            if col_name not in df.columns:
                continue

            def _on_cast_error(value, *, _col=col_name):
                stats.record(column=_col, value=value)

            df[col_name] = df[col_name].map(
                lambda v: perform_cast(v, sa_col.type, on_error=_on_cast_error)
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
    
    @classmethod
    def orm_file_load(cls, ctx: LoaderContext) -> int:
        """
        Load a file into a staging table, delegating chunking to pandas.

        If chunksize is None, pandas returns a single DataFrame, which we
        normalise to a one-element iterator for unified processing.
        """
        total = 0

        delimiter = infer_delim(ctx.path)
        encoding = infer_encoding(ctx.path)['encoding']

        try:
            reader = pd.read_csv(
                ctx.path,
                delimiter=delimiter,
                dtype=str,
                chunksize=ctx.chunksize,
                encoding=encoding,
            )
        except pd.errors.EmptyDataError:
            logger.info(f"File {ctx.path.name} is empty — skipping load for {ctx.tableclass.__tablename__}")
            return 0
        
        logger.info(f"Detected encoding {encoding} for file {ctx.path.name}")
        logger.info(f"Detected delimiter '{delimiter}' for file {ctx.path.name}")       
        chunks = (reader,) if isinstance(reader, pd.DataFrame) else reader

        for chunk in chunks:
            if ctx.normalise:
                chunk = cls.cast_to_model(chunk, ctx)
            if ctx.dedupe:
                chunk = cls.dedupe(chunk, ctx)
            total += cls._load_chunk(
                staging_cls=ctx.staging_table,
                session=ctx.session,
                dataframe=chunk
            )

        return total

class ParquetLoader(LoaderInterface):

    """
    Overhead from this loader is worthwhile with processing and cleaning of very large files.
    """

    @classmethod
    def cast_to_model(cls, data: pa.Table, ctx: LoaderContext) -> pa.Table:
        if data.num_rows == 0:
            return data
        
        table_name = ctx.tableclass.__tablename__
        stats = TableCastingStats(table_name=table_name)
        model_columns = ctx.tableclass.model_columns()
        arrays: dict[str, pa.Array] = {}
        for col_name, sa_col in model_columns.items():
            if col_name not in data.schema.names:
                continue
            arr = data[col_name]

            arrays[col_name] = cast_arrow_column(
                arr,
                sa_col,
                stats=stats,
            )

        out = pa.table(arrays)
        required_cols = [
            name
            for name, col in model_columns.items()
            if not col.nullable and not col.default and not col.server_default
        ]

        if required_cols:
            masks = [pc.is_valid(out[c]) for c in required_cols]            # type: ignore
            valid_mask = reduce(pc.and_, masks)                             # type: ignore

            dropped = out.num_rows - pc.sum(valid_mask).as_py()             # type: ignore
            if dropped > 0:
                logger.warning(
                    "Dropped %d rows with unexpected nulls in %s",
                    dropped,
                    table_name,
                )

            out = out.filter(valid_mask)

        if stats.has_failures():
            for col, col_stats in stats.columns.items():
                logger.warning(f"CAST {table_name}.{col}: {col_stats.count} failures. Examples: {col_stats.examples}")

        return out

    @classmethod
    def dedupe(cls, data: pa.Table, ctx: LoaderContext) -> pa.Table:
        if data.num_rows == 0:
            return data

        pk_names = ctx.tableclass.pk_names()
        deduped = arrow_drop_duplicates(data, pk_names)
        dropped = data.num_rows - deduped.num_rows
        if dropped > 0:
            logger.info(
                "Dropped %d duplicate rows internally for %s",
                dropped,
                ctx.tableclass.__tablename__,
            )

        if not ctx.dedupe_incl_db:
            return deduped
        # todo: make DB-level dedupe for pyarrow
        logger.info(f"DB-level deduplication for ParquetLoader not yet implemented for {ctx.tableclass.__tablename__}")
        return deduped

    @classmethod
    def _scan_batches(cls, ctx: LoaderContext):
        suffix = ctx.path.suffix.lower()
        model_columns = ctx.tableclass.model_columns()
        wanted_cols = list(model_columns.keys())

        if suffix == ".parquet":
            dataset = ds.dataset(ctx.path, format="parquet")
            yield from dataset.to_batches(batch_size=ctx.chunksize or 64_000)

        elif suffix in {".csv", ".tsv"}:
            yield from conservative_load_parquet(ctx.path, wanted_cols=wanted_cols, chunksize=ctx.chunksize)
        else:
            raise ValueError(f"Unsupported file type: {ctx.path}")


    @classmethod
    def orm_file_load(cls, ctx: LoaderContext) -> int:
        total = 0
        for record_batch in cls._scan_batches(ctx):
            if record_batch.num_rows == 0:
                continue
            data: pa.Table | pa.RecordBatch = record_batch
            if ctx.normalise:
                data = cls.cast_to_model(data, ctx=ctx)
            if ctx.dedupe:
                data = cls.dedupe(data, ctx)

            if isinstance(data, pa.RecordBatch):
                df = data.to_pandas()
            elif isinstance(data, pa.Table):
                df = data.to_pandas()
            else:
                df = data 

            if df.empty:
                continue

            total += cls._load_chunk(
                staging_cls=ctx.staging_table,
                session=ctx.session,
                dataframe=df,
            )

        return total