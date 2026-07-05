from __future__ import annotations
from pathlib import Path
import chardet
import csv as _csv
import re
import sqlalchemy as sa
import sqlalchemy.orm as so
import logging
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
import io

_SAFE_ENCODING = re.compile(r'^[A-Za-z][A-Za-z0-9_-]*$')

logger = logging.getLogger(__name__)
COPY_BLOCK_SIZE = 8192

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


def infer_quote_mode(
    path: Path,
    delimiter: str,
    encoding: str = "utf-8",
    sample_rows: int = 2000,
) -> str:
    """Return 'csv' or 'literal' by comparing column-count consistency under both
    quoting interpretations across a sample of rows.

    - 'csv'     → standard RFC-4180 quoting; surrounding double-quotes are stripped
                  and embedded delimiters/newlines inside quotes are preserved.
    - 'literal' → double-quote has no special meaning; every byte is stored as-is.

    Defaults to 'csv' when both modes produce identical output (no quoting in play)
    or when the evidence is tied.  Callers can always override by passing an
    explicit value instead of relying on auto-detection.
    """
    with open(path, encoding=encoding, errors="replace", newline="") as f:
        lines = [f.readline() for _ in range(sample_rows + 1)]

    raw = "".join(ln for ln in lines if ln)
    if not raw:
        return "csv"

    try:
        rows_csv = list(_csv.reader(io.StringIO(raw), delimiter=delimiter))
    except _csv.Error:
        return "literal"

    try:
        rows_lit = list(
            _csv.reader(io.StringIO(raw), delimiter=delimiter, quoting=_csv.QUOTE_NONE)
        )
    except _csv.Error:
        return "csv"

    if not rows_csv:
        return "csv"

    ncols = len(rows_csv[0])
    if ncols <= 1:
        return "csv"

    # No difference between modes → no quoting is active, csv is the safe default
    if rows_csv == rows_lit:
        return "csv"

    data_csv = rows_csv[1:]
    data_lit = rows_lit[1:] if len(rows_lit) > 1 else []

    if not data_csv:
        return "csv"

    csv_ok = sum(1 for r in data_csv if len(r) == ncols)
    lit_ok = sum(1 for r in data_lit if len(r) == ncols)

    # Prefer csv on a tie; only choose literal when it is strictly more consistent
    return "literal" if lit_ok > csv_ok else "csv"


def resolve_quote_mode(
    quote_mode: str,
    path: Path,
    delimiter: str,
    encoding: str = "utf-8",
) -> str:
    """Resolve a requested quote_mode to a concrete ``"csv"`` or ``"literal"``.

    Recognised inputs:

    - ``"csv"`` / ``"literal"`` — returned unchanged (explicit override).
    - ``"auto"`` — legacy content sniff via :func:`infer_quote_mode`. Compares
      column-count consistency under both interpretations across a sample of
      rows. Supports RFC-4180 quoting (embedded delimiters/newlines) but can
      guess wrong when the discriminating rows fall outside the sample.
    - ``"by_delimiter"`` — derive the mode from the delimiter alone, no content
      scan: tab-delimited input never embeds the delimiter in a field so any
      double-quote is literal data (``"literal"``); for non-tab delimiters this
      resolves to ``"csv"`` where RFC-4180 quoting can be meaningful. Deterministic
      and immune to the sampling gamble; use it when the source guarantees tab
      fields never contain embedded tabs/newlines.
    """
    if quote_mode in ("csv", "literal"):
        return quote_mode
    if quote_mode == "by_delimiter":
        return "literal" if delimiter == "\t" else "csv"
    if quote_mode == "auto":
        return infer_quote_mode(path, delimiter=delimiter, encoding=encoding)
    raise ValueError(f"Unknown quote_mode: {quote_mode!r}")


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
    quote_mode: str = "auto",
) -> int:
    raw_conn = session.connection().connection
    if not hasattr(raw_conn, "cursor"):
        raise RuntimeError("Expected DB-API connection for COPY")


    encoding = infer_encoding(path)['encoding'] or 'utf-8'
    if not _SAFE_ENCODING.match(encoding):
        raise ValueError(f"Unsafe encoding value from chardet: {encoding!r}")
    delimiter = infer_delim(path)
    quote_mode = resolve_quote_mode(quote_mode, path, delimiter, encoding)
    logger.info(f"Using quote_mode={quote_mode!r} for {path.name} (delimiter={delimiter!r})")
    if quote_mode == "csv":
        copy_options = f"""
            FORMAT csv,
            HEADER true,
            DELIMITER E'{delimiter}',
            ENCODING '{encoding}'
        """
    elif quote_mode == "literal":
        copy_options = f"""
            FORMAT csv,
            HEADER true,
            DELIMITER E'{delimiter}',
            QUOTE E'\\x01',
            ESCAPE E'\\x01',
            ENCODING '{encoding}'
        """
    else:
        raise ValueError(f"Unknown quote_mode: {quote_mode}")
    
    # Peek at the CSV header to build an explicit column list for COPY.
    # Without this, PostgreSQL expects ALL table columns including internal staging
    # columns like _rownum (GENERATED ALWAYS AS IDENTITY), which the CSV doesn't have.
    with open(path, "rb") as _f_peek:
        _raw_hdr = _f_peek.readline().decode(encoding)
    _nl = check_line_ending(_raw_hdr)
    # _hash is an internal convention for encrypted/hashed columns; strip it so
    # CSV headers map to the base column names that PostgreSQL COPY expects.
    _csv_cols = [c.strip().lower().replace('_hash', '') for c in _raw_hdr.rstrip(_nl).split(delimiter)]
    _cols_sql = ", ".join(f'"{c}"' for c in _csv_cols)

    logger.info(f"Bulk loading {tablename} via COPY (encoding={encoding}, delimiter={delimiter})")

    cur = raw_conn.cursor()
    try:
        with open(path, "rb") as f:
            stream = NormalisedCSVStream(f, encoding=encoding, delimiter=delimiter)
            with cur.copy(
                f'''
                COPY "{tablename}" ({_cols_sql})
                FROM STDIN
                WITH (
                    {copy_options}
                )
                '''
            ) as copy:
                while data := stream.read(COPY_BLOCK_SIZE):
                    copy.write(data)
        session.flush()
        total = session.execute(sa.text(f'SELECT COUNT(*) FROM "{tablename}"')).scalar_one()
        return total
    except Exception as e:
        logger.error(f"Error during bulk load via COPY: {e}")
        session.rollback()
        raise
    finally:
        cur.close()
