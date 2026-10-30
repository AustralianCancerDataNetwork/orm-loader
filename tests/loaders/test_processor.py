"""BatchProcessor: raw batches in, typed rows, rejects and accounting out, no database."""

import csv
from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

from orm_loader.conversion import ConversionPolicy, DateHandler, ErrorPolicy
from orm_loader.helpers import ConversionError, IngestError, RejectLimitError, RequiredValueError
from orm_loader.loaders import LoadReport
from orm_loader.loaders.processor import BatchProcessor
from tests.models import DemoRecord, DemoRequiredRecord

COLUMNS = ["demo_id", "demo_label", "demo_number", "demo_date"]
REJECT = ConversionPolicy(errors=ErrorPolicy(on_error="reject_row"))


def _batch(*rows: tuple, columns: list[str] = COLUMNS) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist([dict(zip(columns, row)) for row in rows])


def _processor(tmp_path: Path, policy: ConversionPolicy | None = None, table=DemoRecord, columns=COLUMNS, **kw):
    report = LoadReport()
    return BatchProcessor(table, columns, policy or ConversionPolicy(), report, tmp_path / "rejects.csv", **kw), report


def _rejects(tmp_path: Path) -> list[dict]:
    with open(tmp_path / "rejects.csv", newline="") as handle:
        return list(csv.DictReader(handle))


def test_raw_text_is_converted_to_the_column_types(tmp_path):
    processor, report = _processor(tmp_path)
    table = processor.process(_batch(("1", "a", "10", "20200229"), ("2", " b ", "", "")))
    assert table.to_pylist() == [
        {"demo_id": 1, "demo_label": "a", "demo_number": 10, "demo_date": date(2020, 2, 29)},
        {"demo_id": 2, "demo_label": "b", "demo_number": None, "demo_date": None},
    ]
    assert report.rows_read == 2


def test_without_normalise_the_batch_passes_through_as_read(tmp_path):
    processor, _ = _processor(tmp_path, normalise=False)
    assert processor.process(_batch(("1", "a", "abc", "x"))).to_pylist() == [
        {"demo_id": "1", "demo_label": "a", "demo_number": "abc", "demo_date": "x"}
    ]


def test_unreadable_values_raise_with_the_count_and_first_three_samples(tmp_path):
    processor, _ = _processor(tmp_path)
    batch = _batch(*[(str(i), "a", bad, "") for i, bad in enumerate(["1", "a", "b", "c", "d", "2"])])
    with pytest.raises(ConversionError) as exc:
        processor.process(batch)
    assert (exc.value.count, exc.value.samples) == (4, ["a", "b", "c"])
    assert "demo_record.demo_number" in str(exc.value)


def test_an_unreadable_date_names_the_option_that_declares_a_format(tmp_path):
    processor, _ = _processor(tmp_path)
    with pytest.raises(ConversionError, match="DateHandler"):
        processor.process(_batch(("1", "a", "", "02/01/2020")))
    declared = ConversionPolicy(date=DateHandler(formats=("dd/mm/yyyy",)))
    processor, _ = _processor(tmp_path, declared)
    assert processor.process(_batch(("1", "a", "", "02/01/2020"))).column("demo_date").to_pylist() == [date(2020, 1, 2)]


def test_a_missing_required_value_raises_with_the_count_and_row_numbers(tmp_path):
    processor, _ = _processor(tmp_path, table=DemoRequiredRecord, columns=["demo_id", "demo_label"])
    with pytest.raises(RequiredValueError) as exc:
        processor.process(_batch(("1", "ok"), ("2", ""), ("3", None), columns=["demo_id", "demo_label"]))
    assert (exc.value.count, exc.value.rows) == (2, [2, 3])


def test_row_numbers_continue_across_batches(tmp_path):
    processor, report = _processor(tmp_path, table=DemoRequiredRecord, columns=["demo_id", "demo_label"])
    processor.process(_batch(("1", "ok"), ("2", "ok"), columns=["demo_id", "demo_label"]))
    with pytest.raises(RequiredValueError) as exc:
        processor.process(_batch(("3", ""), columns=["demo_id", "demo_label"]))
    assert exc.value.rows == [3]
    assert report.rows_read == 3


def test_reject_row_keeps_good_rows_and_writes_the_rest_with_raw_text_and_reason(tmp_path):
    processor, report = _processor(tmp_path, REJECT)
    kept = processor.process(_batch(("1", "a", "10", ""), ("2", "b", "x", ""), ("3", "c", "", "bad date")))
    processor.rejects.close()
    assert kept.column("demo_id").to_pylist() == [1]
    rejected = _rejects(tmp_path)
    assert [(r["row"], r["column"], r["demo_number"], r["demo_date"]) for r in rejected] == [
        ("2", "demo_number", "x", ""),
        ("3", "demo_date", "", "bad date"),
    ]
    assert all("cannot be read" in r["reason"] for r in rejected)
    assert (report.rows_read, report.rows_rejected, report.rejects_path) == (3, 2, tmp_path / "rejects.csv")


def test_reject_row_also_rejects_missing_required_values(tmp_path):
    processor, report = _processor(tmp_path, REJECT, table=DemoRequiredRecord, columns=["demo_id", "demo_label"])
    kept = processor.process(_batch(("1", "ok"), ("2", ""), columns=["demo_id", "demo_label"]))
    processor.rejects.close()
    assert kept.num_rows == 1 and report.rows_rejected == 1
    assert _rejects(tmp_path)[0]["reason"] == "required value is missing"


def test_a_clean_batch_writes_no_rejects_file(tmp_path):
    processor, _ = _processor(tmp_path, REJECT)
    processor.process(_batch(("1", "a", "10", "")))
    processor.rejects.close()
    assert not (tmp_path / "rejects.csv").exists()


@pytest.mark.parametrize("limit, rows_bad, should_stop", [(1, 2, True), (2, 2, False), (0.5, 3, True), (0.5, 2, False)])
def test_max_rejects_stops_a_load_over_a_count_or_a_fraction(tmp_path, limit, rows_bad, should_stop):
    policy = ConversionPolicy(errors=ErrorPolicy(on_error="reject_row", max_rejects=limit))
    processor, _ = _processor(tmp_path, policy)
    rows = [(str(i), "a", "x" if i <= rows_bad else "1", "") for i in range(1, 5)]
    if should_stop:
        with pytest.raises(RejectLimitError):
            processor.process(_batch(*rows))
    else:
        assert processor.process(_batch(*rows)).num_rows == 4 - rows_bad


def test_stream_skips_empty_results_and_closes_the_rejects_file(tmp_path):
    processor, report = _processor(tmp_path, REJECT)
    batches = [_batch(("1", "a", "x", "")), _batch(("2", "b", "5", ""))]
    tables = list(processor.stream(batches))
    assert [t.column("demo_id").to_pylist() for t in tables] == [[2]]
    assert processor.rejects._writer is None
    assert [r["row"] for r in _rejects(tmp_path)] == ["1"]
    assert report.rows_read == 2


def test_an_unwritable_rejects_directory_fails_before_any_batch_is_read(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("")
    with pytest.raises(IngestError, match="not writable"):
        BatchProcessor(DemoRecord, COLUMNS, REJECT, LoadReport(), blocker / "sub" / "rejects.csv")
