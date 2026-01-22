from orm_loader.loaders.data_classes import ColumnCastingStats, TableCastingStats
from orm_loader.loaders.loading_helpers import infer_delim, infer_encoding

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
