from __future__ import annotations
from pathlib import Path
import chardet
import sqlalchemy as sa
import sqlalchemy.orm as so
import logging
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
import io

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

class NormalisedCSVStream(io.RawIOBase):
    def __init__(self, f, encoding: str, delimiter: str):
        self._f = f
        self._encoding = encoding
        self._delimiter = delimiter
        self._sent_header = False
        self._buffer = b""
        self._eof = False

    def readable(self):
        return True

    def read(self, size=-1):
        if self._eof:
            return b""

        out = bytearray()

        # Send rewritten header once
        if not self._sent_header:
            header = self._f.readline().decode(self._encoding)
            newline = check_line_ending(header)
            cols = header.rstrip(newline).split(self._delimiter)
            lowered = [c.lower().replace('_hash', '') for c in cols]
            new_header = (self._delimiter.join(lowered) + "\n").encode(self._encoding)
            out.extend(new_header)
            self._sent_header = True

        while size < 0 or len(out) < size:
            chunk = self._f.read(8192)
            if not chunk:
                self._eof = True
                break

            # Normalize CRLF/CR to LF per chunk
            chunk = chunk.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
            out.extend(chunk)

            if size > 0 and len(out) >= size:
                break

        return bytes(out)

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

def check_line_ending(raw_header: str) -> str:
    if raw_header.endswith("\r\n"):
        return "\r\n"
    elif raw_header.endswith("\n"):
        return "\n"
    elif raw_header.endswith("\r"):
        return "\r"
    else:
        logger.warning("Unable to detect line ending from header: %r. Defaulting to '\\n'", raw_header)
        return "\n"

def quick_load_pg(
    *,
    path: Path,
    session: so.Session,
    tablename: str,
) -> int:
    raw_conn = session.connection().connection  
    if not hasattr(raw_conn, "cursor"):
        raise RuntimeError("Expected DB-API connection for COPY")
    
    encoding = infer_encoding(path)['encoding'] or 'utf-8'
    delimiter = infer_delim(path)

    logger.info(f"Bulk loading {tablename} via COPY (encoding={encoding}, delimiter={delimiter})")
    
    cur = raw_conn.cursor()
    try:
        with open(path, "rb") as f:
            stream = NormalisedCSVStream(f, encoding=encoding, delimiter=delimiter)
            # Read header line to normalise column names to lowercase and detect delimiter/encoding
            # header = f.readline().decode(encoding)
            # newline = check_line_ending(header)
            # cols = header.rstrip(newline).split(delimiter)
            # lowered = [c.lower() for c in cols]

            # if len(set(lowered)) != len(lowered):
            #     raise ValueError(
            #         f"Case-insensitive header collision in {path.name}: {cols}"
            #     )

            # new_header = delimiter.join(lowered) + newline

            # # Reconstruct stream: new header + rest of file
            # rest = f.read()
            # stream = io.BytesIO(new_header.encode(encoding) + rest)

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
                file=stream,
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
