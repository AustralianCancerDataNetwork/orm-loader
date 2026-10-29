"""Detection of the encoding, delimiter and quote mode of a delimited text file."""

from __future__ import annotations

import csv as _csv
import io
import logging
from pathlib import Path

import chardet

logger = logging.getLogger(__name__)

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
