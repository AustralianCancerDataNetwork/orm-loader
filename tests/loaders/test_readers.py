"""File readers: files in, Arrow record batches out, no database."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from orm_loader.conversion import NullPolicy
from orm_loader.loaders import DelimitedTextReader, ParquetReader, open_reader
from tests.models import DemoRecord

COLUMNS = DemoRecord.model_columns()


def _write(tmp_path: Path, text: str, name: str = "demo_record.csv") -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8", newline="")
    return path


def _rows(reader) -> list[dict]:
    return [row for batch in reader.batches() for row in batch.to_pylist()]


def test_open_reader_selects_the_reader_by_suffix(tmp_path):
    pq.write_table(pa.table({"demo_id": [1]}), tmp_path / "demo_record.parquet")
    assert isinstance(open_reader(tmp_path / "demo_record.parquet", COLUMNS), ParquetReader)
    for name in ("demo_record.csv", "demo_record.tsv", "demo_record.txt"):
        assert isinstance(open_reader(_write(tmp_path, "demo_id\n1\n", name), COLUMNS), DelimitedTextReader)


def test_delimited_text_is_read_as_raw_strings_of_the_model_columns_in_file_order(tmp_path):
    path = _write(tmp_path, "demo_label,extra,demo_id\nalpha,x,1\n")
    reader = DelimitedTextReader(path, COLUMNS)
    assert reader.columns == ["demo_label", "demo_id"]
    batch = next(iter(reader.batches()))
    assert batch.schema.names == ["demo_label", "demo_id"]
    assert batch.to_pylist() == [{"demo_label": "alpha", "demo_id": "1"}]


@pytest.mark.parametrize("delimiter", [",", "\t"])
def test_the_delimiter_is_detected(tmp_path, delimiter):
    path = _write(tmp_path, f"demo_id{delimiter}demo_label\n1{delimiter}alpha\n")
    assert _rows(DelimitedTextReader(path, COLUMNS)) == [{"demo_id": "1", "demo_label": "alpha"}]


def test_headers_are_lowercased_and_stripped_of_quotes_and_hash_suffix(tmp_path):
    path = _write(tmp_path, '"DEMO_ID","Demo_Label_hash"\n1,alpha\n')
    assert DelimitedTextReader(path, COLUMNS).columns == ["demo_id", "demo_label"]


def test_an_unquoted_empty_field_is_null_and_a_quoted_one_is_an_empty_string(tmp_path):
    path = _write(tmp_path, 'demo_id,demo_label\n1,\n2,""\n')
    assert [r["demo_label"] for r in _rows(DelimitedTextReader(path, COLUMNS, quote_mode="csv"))] == [None, ""]


def test_embedded_newlines_and_tabs_survive(tmp_path):
    path = _write(tmp_path, 'demo_id,demo_label\n1,"a\nb"\n2,"c\td"\n')
    assert [r["demo_label"] for r in _rows(DelimitedTextReader(path, COLUMNS, quote_mode="csv"))] == ["a\nb", "c\td"]


def test_quote_modes_decide_what_double_quotes_mean(tmp_path):
    path = _write(tmp_path, 'demo_id\tdemo_label\n1\t"x"\n', "demo_record.tsv")
    label = lambda mode: _rows(DelimitedTextReader(path, COLUMNS, quote_mode=mode))[0]["demo_label"]  # noqa: E731
    assert label("csv") == "x"
    assert label("literal") == '"x"'
    assert label("by_delimiter") == '"x"'


def test_a_file_is_read_in_several_batches_without_losing_rows(tmp_path):
    lines = "".join(f"{i},label {i}\n" for i in range(1, 20_001))
    path = _write(tmp_path, "demo_id,demo_label\n" + lines)
    reader = DelimitedTextReader(path, COLUMNS, block_size=64 << 10)
    batches = list(reader.batches())
    assert len(batches) > 1
    assert [row["demo_id"] for batch in batches for row in batch.to_pylist()] == [str(i) for i in range(1, 20_001)]


def test_an_empty_file_has_no_content_and_a_header_only_file_has_no_rows(tmp_path):
    empty = DelimitedTextReader(_write(tmp_path, "", "demo_record.csv"), COLUMNS)
    assert empty.empty and list(empty.batches()) == []
    header_only = DelimitedTextReader(_write(tmp_path, "demo_id,demo_label\n", "other.csv"), COLUMNS)
    assert not header_only.empty and list(header_only.batches()) == []


def test_unquoted_markers_are_nulled_while_reading_but_quoted_ones_are_kept(tmp_path):
    path = _write(tmp_path, 'demo_id,demo_label\n1,NULL\n2,"NULL"\n')
    nulls = NullPolicy(text_markers=frozenset({"NULL"}), marker_quoting="unquoted_only")
    labels = [r["demo_label"] for r in _rows(DelimitedTextReader(path, COLUMNS, nulls, quote_mode="csv"))]
    assert labels == [None, "NULL"]


def test_parquet_keeps_stored_types_and_ignores_other_columns(tmp_path):
    path = tmp_path / "demo_record.parquet"
    pq.write_table(pa.table({"demo_id": [1, 2], "demo_label": ["a", None], "extra": [0, 0]}), path)
    reader = ParquetReader(path, COLUMNS)
    assert reader.columns == ["demo_id", "demo_label"]
    batch = next(iter(reader.batches()))
    assert batch.schema.types == [pa.int64(), pa.string()]
    assert batch.to_pylist() == [{"demo_id": 1, "demo_label": "a"}, {"demo_id": 2, "demo_label": None}]


def test_parquet_is_read_in_batches_of_chunksize_rows(tmp_path):
    path = tmp_path / "demo_record.parquet"
    pq.write_table(pa.table({"demo_id": list(range(250))}), path)
    sizes = [batch.num_rows for batch in ParquetReader(path, COLUMNS, chunksize=100).batches()]
    assert sizes == [100, 100, 50]


def test_parquet_markers_are_nulled_while_reading(tmp_path):
    path = tmp_path / "demo_record.parquet"
    pq.write_table(pa.table({"demo_id": [1, 2], "demo_label": ["NULL", "kept"]}), path)
    nulls = NullPolicy(text_markers=frozenset({"NULL"}), marker_quoting="unquoted_only")
    assert [r["demo_label"] for r in _rows(ParquetReader(path, COLUMNS, nulls))] == [None, "kept"]
