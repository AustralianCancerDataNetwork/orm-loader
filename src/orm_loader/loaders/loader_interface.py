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
from ..constants import RESERVED_COLUMN_DELETE

logger = logging.getLogger(__name__)

_TRUTHY_DELETE: frozenset[str] = frozenset({"true", "1", "t", "yes"})
_FALSY_DELETE: frozenset[str] = frozenset({"false", "0", "f", "no"})


def _normalise_delete_value(v: Any) -> bool | None:
    """
    Convert a raw _delete column value to True, False, or None.

    None means "absent / treat as a normal upsert row".
    Raises ValueError for any value that is not a recognised truthy, falsy,
    or null representation.
    """
    if v is None or v is pd.NA or v is pd.NaT:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        if v == 1:
            return True
        if v == 0:
            return False
    if isinstance(v, str):
        s = v.strip().lower()
        if s in _TRUTHY_DELETE:
            return True
        if s in _FALSY_DELETE:
            return False
    raise ValueError(
        f"Invalid value {v!r} in column '{RESERVED_COLUMN_DELETE}'. "
        f"Expected one of: true, false, 1, 0, t, f, yes, no (or null/absent)."
    )


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

@staticmethod
def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower().replace('_hash', '').strip() for c in df.columns]
    return df

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
        if ctx.has_delete_column and RESERVED_COLUMN_DELETE in df.columns:
            # sort delete=True rows last per PK group so keep='last' keeps delete rows
            df = df.copy()
            df['_delete_sort'] = df[RESERVED_COLUMN_DELETE].map(lambda v: 1 if v is True else 0)
            df = df.sort_values(pk_names + ['_delete_sort'])
            df = df.drop_duplicates(subset=pk_names, keep='last')
            df = df.drop(columns=['_delete_sort'])
        else:
            df = df.drop_duplicates(subset=pk_names, keep='first')
        dropped_internal = before - len(df)
        if dropped_internal > 0:
            logger.info(f"Dropped {dropped_internal} duplicate rows internally in staging for {ctx.tableclass.__tablename__}")
        return df.copy()

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
                    logger.warning(f"Found {null_count} rows with unexpected nulls in {table_name}.{col}")
            # Drop rows violating required constraints, but always preserve delete-marked rows
            # (they only need PK columns — non-PK values are irrelevant for deletion).
            if ctx.has_delete_column and RESERVED_COLUMN_DELETE in df.columns:
                is_delete_row = df[RESERVED_COLUMN_DELETE] == True  # noqa: E712
                df = df.loc[~null_mask.any(axis=1) | is_delete_row]
            else:
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
        logger.info(f"Loading with chunksize '{ctx.chunksize}' for file {ctx.path.name}")       
        chunks = (reader,) if isinstance(reader, pd.DataFrame) else reader

        total = 0
        for i, chunk in enumerate(chunks):
            logger.debug(f"Processing chunk {i} with {len(chunk)} rows for {ctx.tableclass.__tablename__}")
            chunk = _normalise_columns(chunk)
            if ctx.has_delete_column and RESERVED_COLUMN_DELETE in chunk.columns:
                chunk = chunk.copy()
                chunk[RESERVED_COLUMN_DELETE] = chunk[RESERVED_COLUMN_DELETE].map(_normalise_delete_value)
            if ctx.dedupe:
                chunk = cls.dedupe(chunk, ctx)
            if ctx.normalise:
                chunk = cls.cast_to_model(chunk, ctx)
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

        valid_mask = None
        if required_cols:
            masks = [pc.is_valid(out[c]) for c in required_cols]            # type: ignore
            valid_mask = reduce(pc.and_, masks)                             # type: ignore

            if ctx.has_delete_column and RESERVED_COLUMN_DELETE in data.schema.names:
                # preserve delete-marked rows — their non-PK values are irrelevant
                is_delete = pc.fill_null(data[RESERVED_COLUMN_DELETE], False)  # type: ignore
                valid_mask = pc.or_(valid_mask, is_delete)                     # type: ignore

            dropped = out.num_rows - pc.sum(valid_mask).as_py()             # type: ignore
            if dropped > 0:
                logger.warning(f"Dropped {dropped} rows with unexpected nulls in {table_name}")
            out = out.filter(valid_mask)

        if ctx.has_delete_column and RESERVED_COLUMN_DELETE in data.schema.names:
            delete_arr = data[RESERVED_COLUMN_DELETE]
            if valid_mask is not None:
                delete_arr = delete_arr.filter(valid_mask)
            out = out.append_column(RESERVED_COLUMN_DELETE, delete_arr)

        if stats.has_failures():
            for col, col_stats in stats.columns.items():
                logger.warning(f"CAST {table_name}.{col}: {col_stats.count} failures. Examples: {col_stats.examples}")

        return out

    @classmethod
    def dedupe(cls, data: pa.Table, ctx: LoaderContext) -> pa.Table:
        if data.num_rows == 0:
            return data

        pk_names = ctx.tableclass.pk_names()

        if ctx.has_delete_column and RESERVED_COLUMN_DELETE in data.schema.names:
            delete_col = data[RESERVED_COLUMN_DELETE]
            delete_sort = pc.cast(pc.fill_null(delete_col, False), pa.int8())  # type: ignore
            data_with_sort = data.append_column("_delete_sort", delete_sort)
            sort_keys = [(name, "ascending") for name in pk_names] + [("_delete_sort", "ascending")]
            sorted_idx = pc.sort_indices(data_with_sort, sort_keys=sort_keys)  # type: ignore
            sorted_data = data_with_sort.take(sorted_idx)

            # keep last per PK group: row i is the last of its group when next row has different PK
            diffs = []
            for name in pk_names:
                col = sorted_data[name]
                diffs.append(pc.not_equal(col[:-1], col[1:]))  # type: ignore
            keep_tail = diffs[0]
            for d in diffs[1:]:
                keep_tail = pc.or_(keep_tail, d)  # type: ignore
            keep_tail = pc.fill_null(keep_tail, True)  # type: ignore
            if isinstance(keep_tail, pa.ChunkedArray):
                keep_tail = keep_tail.combine_chunks()
            keep = pa.concat_arrays([keep_tail, pa.array([True], type=pa.bool_())])

            deduped_with_sort = sorted_data.filter(keep)
            sort_col_idx = deduped_with_sort.schema.get_field_index("_delete_sort")
            deduped = deduped_with_sort.remove_column(sort_col_idx)
        else:
            deduped = arrow_drop_duplicates(data, pk_names)

        dropped = data.num_rows - deduped.num_rows
        if dropped > 0:
            logger.info(f"Dropped {dropped} duplicate rows internally for {ctx.tableclass.__tablename__}")
        return deduped

    @classmethod
    def _scan_batches(cls, ctx: LoaderContext):
        suffix = ctx.path.suffix.lower()
        model_columns = ctx.tableclass.model_columns()
        wanted_cols = list(model_columns.keys())
        logger.info(f"Scanning batches for {ctx.tableclass.__tablename__}")
        if suffix == ".parquet":
            dataset = ds.dataset(ctx.path, format="parquet")
            yield from dataset.to_batches(batch_size=ctx.chunksize or 64_000)

        elif suffix in {".csv", ".tsv"}:
            if ctx.has_delete_column:
                wanted_cols = wanted_cols + [RESERVED_COLUMN_DELETE]
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

            if isinstance(data, pa.RecordBatch) or isinstance(data, pa.Table):
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