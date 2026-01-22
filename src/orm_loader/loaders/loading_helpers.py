from __future__ import annotations
from pathlib import Path
import chardet
import sqlalchemy as sa
import sqlalchemy.orm as so
import logging
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv

logger = logging.getLogger(__name__)

"""
Loader Helper Functions
=======================

Utility functions supporting file loading and ingestion workflows.

Includes helpers for:
- delimiter and encoding detection
- conservative CSV parsing via pyarrow
- duplicate detection in columnar data
- PostgreSQL COPY-based bulk loading

These helpers are intentionally low-level and stateless.
"""


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


def arrow_drop_duplicates(
    table: pa.Table,
    pk_names: list[str],
) -> pa.Table:
    if table.num_rows == 0:
        return table

    sort_keys = [(name, "ascending") for name in pk_names]
    sorted_idx = pc.sort_indices(table, sort_keys=sort_keys)    # type: ignore
    sorted_table = table.take(sorted_idx)
    diffs = []
    for name in pk_names:
        col = sorted_table[name]
        previous_arr = col[:-1]
        this_arr = col[1:]
        diffs.append(
            pc.not_equal(previous_arr, this_arr)                # type: ignore
        )
    keep_tail = diffs[0]
    for d in diffs[1:]:
        keep_tail = pc.or_(keep_tail, d)                        # type: ignore
    keep = pc.fill_null(keep_tail, True)
    if isinstance(keep, pa.ChunkedArray):
        keep = keep.combine_chunks()
    keep = pa.concat_arrays([
        pa.array([True], type=pa.bool_()),
        keep,
    ])
    deduped = sorted_table.filter(keep)
    
    return deduped


def conservative_load_parquet(path: Path, wanted_cols: list[str], chunksize: int | None = None) -> pa.Table:
    delimiter = infer_delim(path)
    encoding = infer_encoding(path)["encoding"]
    convert_opts = pv.ConvertOptions(
        strings_can_be_null=True,                
        include_columns=wanted_cols,
    )

    def _invalid_row_handler(row):
        logger.warning("Skipping malformed CSV row: %r", row[:200])
        return "skip"
    
    parse_opts = pv.ParseOptions(
        delimiter=delimiter,
        ignore_empty_lines=True,
        quote_char=False,
        invalid_row_handler=_invalid_row_handler
    )
    read_opts = pv.ReadOptions(
        block_size=chunksize or 64_000,
        encoding=encoding,
        use_threads=True,
    )
    if chunksize:
        read_opts.block_size = chunksize
    with pv.open_csv(
        path,
        read_options=read_opts,
        parse_options=parse_opts,
        convert_options=convert_opts,
    ) as reader:
        for batch in reader:
            yield batch

def quick_load_pg(
    *,
    path: Path,
    session: so.Session,
    tablename: str,
) -> int:
    raw_conn = session.connection().connection  
    if not hasattr(raw_conn, "cursor"):
        raise RuntimeError("Expected DB-API connection for COPY")
    
    encoding = infer_encoding(path)['encoding']
    delimiter = infer_delim(path)

    logger.info(f"Bulk loading {tablename} via COPY (encoding={encoding}, delimiter={delimiter})")
    
    cur = raw_conn.cursor()
    try:
        with open(path, "rb") as f:
            cur.copy_expert(
                sql=f'''
                COPY "{tablename}"
                FROM STDIN
                WITH (
                    FORMAT csv,
                    HEADER true,
                    DELIMITER E'{delimiter}',
                    ENCODING '{encoding}'
                )
                ''',
                file=f,
            )
        session.flush()
        total = session.execute(sa.text(f'SELECT COUNT(*) FROM "{tablename}"')).scalar_one()
        return total
    except Exception as e:
        logger.error(f"Error during bulk load via COPY: {e}")
        session.rollback()
        raise
    finally:
        cur.close()
