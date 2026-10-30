import pytest

from orm_loader.loaders.readers.detection import (
    infer_delim,
    infer_encoding,
    resolve_quote_mode,
)


def test_infer_delim_csv(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("a,b,c\n1,2,3\n")
    assert infer_delim(p) == ","

def test_infer_delim_tsv(tmp_path):
    p = tmp_path / "x.tsv"
    p.write_text("a\tb\tc\n1\t2\t3\n")
    assert infer_delim(p) == "\t"

def test_infer_encoding_promotes_ascii_to_utf8(tmp_path):
    p = tmp_path / "ascii.csv"
    p.write_bytes(b"id,name\n1,alpha\n")
    assert infer_encoding(p)["encoding"] == "utf-8"


def test_infer_encoding_utf8(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("hello")
    enc = infer_encoding(p).get("encoding") or ""
    assert enc.lower() in {"utf-8", "ascii"}


# ---------------------------------------------------------------------------
# resolve_quote_mode
# ---------------------------------------------------------------------------

def test_resolve_quote_mode_explicit_csv_passthrough():
    # Explicit modes are returned unchanged and never read the file
    assert resolve_quote_mode("csv", ",") == "csv"


def test_resolve_quote_mode_explicit_literal_passthrough():
    assert resolve_quote_mode("literal", "\t") == "literal"


def test_resolve_quote_mode_by_delimiter_tab_is_literal():
    # Tab-delimited input never embeds the delimiter in a field, so any
    # double-quote is literal data — derived from the delimiter, no file scan
    assert resolve_quote_mode("by_delimiter", "\t") == "literal"


def test_resolve_quote_mode_by_delimiter_comma_is_csv():
    # Comma-delimited input must wrap fields containing the delimiter in quotes,
    # so quoting is meaningful
    assert resolve_quote_mode("by_delimiter", ",") == "csv"


def test_resolve_quote_mode_auto_is_not_a_mode():
    with pytest.raises(ValueError, match="Unknown quote_mode"):
        resolve_quote_mode("auto", "\t")


def test_resolve_quote_mode_unknown_raises():
    with pytest.raises(ValueError, match="Unknown quote_mode"):
        resolve_quote_mode("bogus", ",")
