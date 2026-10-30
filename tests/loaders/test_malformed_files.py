"""Files that are broken as a whole: ragged lines, undecodable bytes, stale rejects and ignored columns."""

import logging
from pathlib import Path
from typing import Type, cast

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa

from orm_loader.backends import STAGING_SCHEMA, resolve_backend
from orm_loader.conversion import ConversionPolicy, ErrorPolicy
from orm_loader.helpers import IngestError, PolicyError
from orm_loader.loaders import DelimitedTextReader, LoadReport, ParquetReader
from orm_loader.tables.typing import FileTableProtocol
from tests.models import DemoBinaryRecord, DemoRecord, DemoRequiredRecord

_Demo = cast(Type[FileTableProtocol], DemoRecord)
_DemoRequired = cast(Type[FileTableProtocol], DemoRequiredRecord)
REJECT = ConversionPolicy(errors=ErrorPolicy(on_error="reject_row"))
COLUMNS = DemoRecord.model_columns()


def _load(session, tmp_path: Path, text: str, **kwargs) -> int:
    path = tmp_path / "demo_record.csv"
    path.write_text(text, encoding="utf-8", newline="")
    tag = None if session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA
    loaded = _Demo.load_file(session, path, staging_schema_tag=tag, **kwargs)
    session.commit()
    return loaded


def _ids(session) -> list[int]:
    return [r[0] for r in session.execute(sa.select(DemoRecord.demo_id).order_by(DemoRecord.demo_id))]


def _rejects(path: Path) -> list[dict]:
    options = pv.ConvertOptions(column_types={"row": pa.int64(), "column": pa.string(), "reason": pa.string()})
    return pv.read_csv(path, convert_options=options).to_pylist()


RAGGED = "demo_id,demo_label\n1,a\n2\n3,c,extra\n4,d\n"


def test_a_ragged_line_fails_the_read_by_default(dialect_session, tmp_path):
    with pytest.raises(IngestError, match="demo_record.csv.*Expected 2 columns"):
        _load(dialect_session, tmp_path, RAGGED)


def test_ragged_lines_are_rejected_with_their_text_under_reject_row(dialect_session, tmp_path):
    report = LoadReport()
    assert _load(dialect_session, tmp_path, RAGGED, policy=REJECT, report=report) == 2
    assert _ids(dialect_session) == [1, 4]
    assert report.rows_read == 4 and report.rows_rejected == 2 and report.accounted
    rejects = _rejects(report.rejects_path)
    assert [r["reason"] for r in rejects] == [
        "expected 2 columns, got 1: 2",
        "expected 2 columns, got 3: 3,c,extra",
    ]
    assert [r["row"] for r in rejects] == [2, 3] and not any(r["column"] for r in rejects)


def test_row_numbers_count_the_malformed_lines_before_them(dialect_session, tmp_path):
    report = LoadReport()
    text = "demo_id,demo_label,demo_number\n1,a,1\n2,b\n3,c,3\n4,d,x\n5,e,5\n"
    assert _load(dialect_session, tmp_path, text, policy=REJECT, report=report) == 3
    assert [(r["row"], r["column"]) for r in _rejects(report.rejects_path)] == [(2, ""), (4, "demo_number")]


def test_a_required_value_error_names_the_row_in_the_file(dialect_session, tmp_path):
    from orm_loader.helpers import RequiredValueError
    from tests.models import DemoRequiredRecord

    path = tmp_path / "demo_required_record.csv"
    path.write_text("demo_id,demo_label\n1,a\n2\n3,\n")
    reader = DelimitedTextReader(path, DemoRequiredRecord.model_columns(), skip_malformed=True)
    with pytest.raises(RequiredValueError) as error:
        _DemoRequired.load_file(dialect_session, path, reader=reader, policy=ConversionPolicy())
    assert error.value.rows == [3]


def test_a_file_of_only_ragged_lines_is_rejected_not_silently_empty(dialect_session, tmp_path):
    report = LoadReport()
    assert _load(dialect_session, tmp_path, "demo_id,demo_label\n1\n2\n", policy=REJECT, report=report) == 0
    assert report.rows_read == 2 and report.rows_rejected == 2 and report.accounted


def test_undecodable_bytes_fail_with_the_file_name(tmp_path):
    path = tmp_path / "demo_record.csv"
    path.write_bytes(b"demo_id,demo_label\n1,a\x81\n")
    reader = DelimitedTextReader(path, COLUMNS)
    reader.encoding = "cp1252"
    with pytest.raises(IngestError, match="demo_record.csv"):
        list(reader.batches())


def test_a_clean_reload_removes_the_rejects_file_of_the_previous_load(dialect_session, tmp_path):
    first = LoadReport()
    _load(dialect_session, tmp_path, RAGGED, policy=REJECT, report=first)
    assert first.rejects_path.exists()
    second = LoadReport()
    _load(dialect_session, tmp_path, "demo_id,demo_label\n1,a\n", policy=REJECT, report=second)
    assert second.rejects_path is None
    assert not first.rejects_path.exists()


def test_a_failed_read_leaves_no_staging_table_behind(dialect_session, tmp_path):
    with pytest.raises(IngestError):
        _load(dialect_session, tmp_path, RAGGED)
    backend = resolve_backend(dialect_session, mapper=DemoRecord)
    name = backend.staging_name_for_table(DemoRecord.__tablename__)
    schema = None if dialect_session.get_bind().dialect.name == "sqlite" else backend.staging_schema
    assert not sa.inspect(dialect_session.get_bind()).has_table(name, schema=schema)


def test_file_columns_the_table_lacks_are_reported(dialect_session, tmp_path, caplog):
    with caplog.at_level(logging.WARNING):
        _load(dialect_session, tmp_path, "demo_id,demo_labl,demo_label\n1,typo,a\n")
    assert "which are ignored: ['demo_labl']" in caplog.text


def test_parquet_column_names_are_normalised_like_delimited_headers(tmp_path):
    path = tmp_path / "demo_record.parquet"
    pq.write_table(pa.table({"Demo_ID": [1], '"DEMO_LABEL_hash"': ["a"], "Other": [1]}), path)
    reader = ParquetReader(path, COLUMNS)
    assert reader.columns == ["demo_id", "demo_label"]
    assert reader.file_columns == ["demo_id", "demo_label", "other"]
    assert [b.schema.names for b in reader.batches()] == [["demo_id", "demo_label"]]


def test_a_hash_suffix_is_stripped_only_at_the_end_of_a_name(tmp_path):
    path = tmp_path / "demo_record.csv"
    path.write_text("demo_id,my_hash_value,demo_label_hash\n1,x,a\n")
    assert DelimitedTextReader(path, COLUMNS).file_columns == ["demo_id", "my_hash_value", "demo_label"]


def test_duplicate_names_after_normalisation_are_an_error(tmp_path):
    path = tmp_path / "demo_record.csv"
    path.write_text("demo_id,Demo_ID\n1,2\n")
    with pytest.raises(IngestError, match="duplicate column names"):
        DelimitedTextReader(path, COLUMNS)


def test_invalid_utf8_is_reported_without_the_quote_mode_hint(tmp_path):
    path = tmp_path / "demo_record.csv"
    path.write_bytes(b"demo_id,demo_label\n1,a\xff\n")
    reader = DelimitedTextReader(path, COLUMNS)
    reader.encoding = "utf-8"
    with pytest.raises(IngestError, match="demo_record.csv") as error:
        list(reader.batches())
    assert "quote_mode" not in str(error.value)


def test_a_column_type_without_a_handler_is_fine_when_values_are_not_interpreted(dialect_session, tmp_path):
    path = tmp_path / "demo_binary_record.csv"
    path.write_text("demo_id,demo_blob\n1,abc\n")
    tag = None if dialect_session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA
    table = cast(Type[FileTableProtocol], DemoBinaryRecord)
    with pytest.raises(PolicyError, match="demo_blob"):
        table.load_file(dialect_session, path, staging_schema_tag=tag)
    assert table.load_file(dialect_session, path, staging_schema_tag=tag, normalise=False) == 1


@pytest.mark.parametrize("option", [{"quote_mode": "csv"}, {"chunksize": 10}])
def test_reader_options_alongside_a_reader_are_refused(dialect_session, tmp_path, option):
    path = tmp_path / "demo_record.csv"
    path.write_text("demo_id,demo_label,demo_number\n1,a,1\n")
    reader = DelimitedTextReader(path, DemoRecord.model_columns())
    with pytest.raises(ValueError, match="configure the default reader"):
        _Demo.load_file(dialect_session, path, reader=reader, **option)
