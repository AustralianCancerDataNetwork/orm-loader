"""Detection of the encoding, delimiter and quote mode of a delimited text file."""

from __future__ import annotations

import logging

import chardet

logger = logging.getLogger(__name__)

def infer_encoding(file):
    with open(file, 'rb') as infile:
        encoding = chardet.detect(infile.read(10000))
    if encoding['encoding'] == 'ascii':
        encoding['encoding'] = 'utf-8' # utf-8 valid superset of ascii, so being more conservative here just because it flakes occasionally
    return encoding

def infer_delim(file):
    with open(file, 'rb') as infile:
        line = infile.readline()
    return '\t' if line.count(b'\t') > line.count(b',') else ','


def resolve_quote_mode(quote_mode: str, delimiter: str) -> str:
    """Resolve a requested quote_mode to a concrete ``"csv"`` or ``"literal"``.

    Parameters
    ----------
    quote_mode
        ``"csv"`` reads double quotes as RFC 4180 field quoting. ``"literal"``
        keeps them as data. ``"by_delimiter"`` chooses ``"literal"`` for tab
        delimited files, where a field never embeds the delimiter, and
        ``"csv"`` otherwise.
    delimiter
        Delimiter of the file.

    Returns
    -------
    str
        ``"csv"`` or ``"literal"``.

    Raises
    ------
    ValueError
        ``quote_mode`` is not one of the above.
    """
    if quote_mode in ("csv", "literal"):
        return quote_mode
    if quote_mode == "by_delimiter":
        return "literal" if delimiter == "\t" else "csv"
    raise ValueError(f"Unknown quote_mode: {quote_mode!r}")
