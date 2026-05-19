from orm_loader.loaders.data_classes import ColumnCastingStats, TableCastingStats
from orm_loader.loaders.loading_helpers import infer_delim, infer_encoding, infer_quote_mode

def test_column_casting_stats_records_examples():
    stats = ColumnCastingStats()
    stats.record("bad1")
    stats.record("bad2")
    stats.record("bad3")
    stats.record("bad4")  # should not be stored

    assert stats.count == 4
    assert len(stats.examples) == 3

def test_table_casting_stats_aggregation():
    stats = TableCastingStats(table_name="test")
    stats.record(column="a", value="x")
    stats.record(column="a", value="y")
    stats.record(column="b", value="z")

    assert stats.total_failures == 3
    assert stats.has_failures() is True

def test_infer_delim_csv(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("a,b,c\n1,2,3\n")
    assert infer_delim(p) == ","

def test_infer_delim_tsv(tmp_path):
    p = tmp_path / "x.tsv"
    p.write_text("a\tb\tc\n1\t2\t3\n")
    assert infer_delim(p) == "\t"

def test_infer_encoding_utf8(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("hello")
    enc = infer_encoding(p).get("encoding") or ""
    assert enc.lower() in {"utf-8", "ascii"}


def test_infer_quote_mode_unquoted_tsv_returns_csv(tmp_path):
    # No quotes anywhere: both modes identical, csv is the safe default
    p = tmp_path / "x.csv"
    p.write_text("id\tname\tvalue\n1\tAlice\t10\n2\tBob\t20\n")
    assert infer_quote_mode(p, delimiter="\t") == "csv"


def test_infer_quote_mode_rfc4180_quoted_field_returns_csv(tmp_path):
    # Athena-style: quoted concept_name at the varchar(255) boundary,
    # no embedded delimiter — the column-count tie-break must favour csv
    p = tmp_path / "x.csv"
    long_name = "A" * 255
    p.write_text(f'id\tname\n1\t"{long_name}"\n2\tnormal\n')
    assert infer_quote_mode(p, delimiter="\t") == "csv"


def test_infer_quote_mode_embedded_delimiter_in_quoted_field_returns_csv(tmp_path):
    # Quoted field contains the delimiter: csv mode keeps column count consistent,
    # literal mode splits on the embedded tab and produces ragged rows
    p = tmp_path / "x.csv"
    p.write_text('id\tname\tval\n1\t"foo\tbar"\t99\n2\tbaz\t0\n')
    assert infer_quote_mode(p, delimiter="\t") == "csv"


def test_infer_quote_mode_unbalanced_quote_returns_literal(tmp_path):
    # Unbalanced leading quote breaks CSV parsing: literal mode produces
    # consistent 2-column rows while csv mode does not
    p = tmp_path / "x.csv"
    p.write_text('id\tname\n1\t"open\n2\t"open\n3\t"open\n')
    assert infer_quote_mode(p, delimiter="\t") == "literal"
