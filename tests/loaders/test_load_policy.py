"""Loading under a null policy: markers, rejected rows, row accounting and sources."""

import logging
import os
from datetime import date
from pathlib import Path
from typing import Type, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa

from orm_loader.backends import STAGING_SCHEMA
from orm_loader.conversion import ConversionPolicy, DateHandler, ErrorPolicy, NullPolicy, TextHandler
from orm_loader.helpers import ConversionError, IngestError, RejectLimitError, RequiredValueError
from orm_loader.backends import DedupeResult
from orm_loader.loaders import LoadReport
from orm_loader.tables.typing import FileTableProtocol
from tests.models import DemoRecord, DemoRequiredRecord

_Demo = cast(Type[FileTableProtocol], DemoRecord)
_DemoRequired = cast(Type[FileTableProtocol], DemoRequiredRecord)

SEVEN_ROWS = 'demo_id,demo_label\n1,NULL\n2,"NULL"\n3,"""NULL"""\n4,\n5,""\n6,"alpha,beta"\n7,"alpha\nbeta"\n'
MARKER = ConversionPolicy(nulls=NullPolicy(text_markers=frozenset({"NULL"})))


def _load(dialect_session, tmp_path: Path, table, text: str, name: str | None = None, **kwargs) -> int:
    path = tmp_path / f"{name or table.__tablename__}.csv"
    path.write_text(text, encoding="utf-8", newline="")
    tag = None if dialect_session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA
    loaded = table.load_file(dialect_session, path, staging_schema_tag=tag, **kwargs)
    dialect_session.commit()
    dialect_session.expire_all()
    return loaded


def _labels(dialect_session, model) -> dict[int, str | None]:
    rows = dialect_session.execute(sa.select(model.demo_id, model.demo_label).order_by(model.demo_id)).all()
    return {r[0]: r[1] for r in rows}


def test_text_marker_is_missing_whether_or_not_quoted(dialect_session, tmp_path):
    _load(dialect_session, tmp_path, _Demo, SEVEN_ROWS, policy=MARKER)
    assert _labels(dialect_session, DemoRecord) == {
        1: None, 2: None, 3: '"NULL"', 4: None, 5: None, 6: "alpha,beta", 7: "alpha\nbeta",
    }


def test_text_marker_matches_exactly_and_ignores_padding(dialect_session, tmp_path):
    text = "demo_id,demo_label\n1,NULL\n2, NULL \n3,Null\n4,null\n5,NULLIFY\n6,Contains NULL here\n"
    _load(dialect_session, tmp_path, _Demo, text, policy=MARKER)
    assert _labels(dialect_session, DemoRecord) == {
        1: None, 2: None, 3: "Null", 4: "null", 5: "NULLIFY", 6: "Contains NULL here",
    }


def test_marker_in_a_required_column_raises(dialect_session, tmp_path):
    text = "demo_id,demo_label\n1,ok\n2,NULL\n"
    with pytest.raises(RequiredValueError, match="demo_label"):
        _load(dialect_session, tmp_path, _DemoRequired, text, policy=MARKER)


def test_column_markers_apply_only_to_their_column(dialect_session, tmp_path):
    policy = ConversionPolicy(columns={("demo_record", "demo_label"): TextHandler(markers=frozenset({"N/A"}))})
    _load(dialect_session, tmp_path, _Demo, "demo_id,demo_label\n1,N/A\n2,NULL\n", policy=policy)
    assert _labels(dialect_session, DemoRecord) == {1: None, 2: "NULL"}


def test_quote_sensitive_marker_matches_only_unquoted(dialect_session, tmp_path):
    policy = ConversionPolicy(nulls=NullPolicy(text_markers=frozenset({"NULL"}), marker_quoting="unquoted_only"))
    _load(dialect_session, tmp_path, _Demo, SEVEN_ROWS, policy=policy)
    assert _labels(dialect_session, DemoRecord) == {
        1: None, 2: "NULL", 3: '"NULL"', 4: None, 5: None, 6: "alpha,beta", 7: "alpha\nbeta",
    }


def test_quote_sensitive_marker_warns_when_every_field_is_quoted(dialect_session, tmp_path, caplog):
    policy = ConversionPolicy(nulls=NullPolicy(text_markers=frozenset({"NULL"}), marker_quoting="unquoted_only"))
    text = '"demo_id","demo_label"\n"1","NULL"\n"2","x"\n'
    with caplog.at_level(logging.WARNING):
        _load(dialect_session, tmp_path, _Demo, text, policy=policy)
    assert "quotes every field" in caplog.text
    assert _labels(dialect_session, DemoRecord) == {1: "NULL", 2: "x"}


def test_quote_sensitive_markers_reject_column_markers():
    with pytest.raises(ValueError, match="unquoted_only"):
        ConversionPolicy(
            nulls=NullPolicy(marker_quoting="unquoted_only"),
            columns={("t", "c"): TextHandler(markers=frozenset({"x"}))},
        )


def test_both_athena_date_formats_and_iso_load_in_one_file(dialect_session, tmp_path):
    text = "demo_id,demo_date\n1,20200229\n2,01-JAN-2020\n3,2020-01-02\n4,\n5,NA\n"
    _load(dialect_session, tmp_path, _Demo, text)
    rows = dialect_session.execute(sa.select(DemoRecord.demo_id, DemoRecord.demo_date).order_by(DemoRecord.demo_id)).all()
    assert [tuple(r) for r in rows] == [
        (1, date(2020, 2, 29)), (2, date(2020, 1, 1)), (3, date(2020, 1, 2)), (4, None), (5, None),
    ]


def test_ambiguous_date_needs_a_declared_format(dialect_session, tmp_path):
    text = "demo_id,demo_date\n1,02/01/2020\n"
    with pytest.raises(ConversionError, match="DateHandler"):
        _load(dialect_session, tmp_path, _Demo, text)
    _load(dialect_session, tmp_path, _Demo, text, policy=ConversionPolicy(date=DateHandler(formats=("dd/mm/yyyy",))))
    assert dialect_session.execute(sa.select(DemoRecord.demo_date)).scalar_one() == date(2020, 1, 2)


MESSY = (
    "demo_id,demo_label,demo_number,demo_date\n"
    "1,ok,1,20200101\n"
    "2,,2,20200102\n"
    "3,bad number,x,20200103\n"
    "4,bad date,4,20200231\n"
    "5,fine,5,20200105\n"
)


def _rejects(path: Path) -> list[dict]:
    import pyarrow.csv as pv

    header = [h.strip('"') for h in path.read_text().splitlines()[0].split(",")]
    options = pv.ConvertOptions(column_types={name: pa.string() for name in header})
    return pv.read_csv(path, convert_options=options).to_pylist()


def test_reject_row_loads_good_rows_and_writes_the_rest(dialect_session, tmp_path):
    report = LoadReport()
    policy = ConversionPolicy(errors=ErrorPolicy(on_error="reject_row"))
    loaded = _load(dialect_session, tmp_path, _Demo, MESSY, policy=policy, report=report)
    assert loaded == 3
    assert _labels(dialect_session, DemoRecord) == {1: "ok", 2: None, 5: "fine"}
    assert (report.rows_read, report.rows_loaded, report.rows_rejected) == (5, 3, 2)
    assert report.accounted
    assert report.rejects_path == tmp_path / "rejects" / "demo_record.rejects.csv"
    rejected = _rejects(report.rejects_path)
    assert [(r["row"], r["column"], r["demo_number"], r["demo_date"]) for r in rejected] == [
        ("3", "demo_number", "x", "20200103"),
        ("4", "demo_date", "4", "20200231"),
    ]
    assert rejected[0]["reason"] == "cannot be read as Integer"


def test_reject_row_also_rejects_missing_required_values(dialect_session, tmp_path):
    report = LoadReport()
    text = "demo_id,demo_label\n1,ok\n2,\n3,NA\n"
    loaded = _load(
        dialect_session, tmp_path, _DemoRequired, text, policy=ConversionPolicy(errors=ErrorPolicy(on_error="reject_row")), report=report
    )
    assert loaded == 2
    assert _labels(dialect_session, DemoRequiredRecord) == {1: "ok", 3: "NA"}
    [rejected] = _rejects(report.rejects_path)
    assert (rejected["row"], rejected["column"], rejected["reason"]) == ("2", "demo_label", "required value is missing")


def test_rejects_go_to_the_requested_directory(dialect_session, tmp_path):
    target = tmp_path / "elsewhere"
    report = LoadReport()
    _load(
        dialect_session, tmp_path, _Demo, MESSY,
        policy=ConversionPolicy(errors=ErrorPolicy(on_error="reject_row")), rejects_dir=target, report=report,
    )
    assert report.rejects_path == target / "demo_record.rejects.csv"
    assert len(_rejects(report.rejects_path)) == 2
    assert not (tmp_path / "rejects").exists()


def test_unwritable_rejects_directory_fails_before_loading(dialect_session, tmp_path):
    blocked = tmp_path / "blocked"
    blocked.mkdir()
    blocked.chmod(0o500)
    try:
        if os.access(blocked, os.W_OK):
            pytest.skip("directory permissions are not enforced for this user")
        with pytest.raises(IngestError, match="not writable"):
            _load(
                dialect_session, tmp_path, _Demo, MESSY,
                policy=ConversionPolicy(errors=ErrorPolicy(on_error="reject_row")), rejects_dir=blocked / "inner",
            )
    finally:
        blocked.chmod(0o700)
    assert dialect_session.execute(sa.select(sa.func.count()).select_from(DemoRecord)).scalar_one() == 0


def test_clean_file_writes_no_rejects_file(dialect_session, tmp_path):
    report = LoadReport()
    _load(
        dialect_session, tmp_path, _Demo, "demo_id,demo_label\n1,a\n",
        policy=ConversionPolicy(errors=ErrorPolicy(on_error="reject_row")), report=report,
    )
    assert report.rejects_path is None
    assert not (tmp_path / "rejects").exists()


@pytest.mark.parametrize("limit", [1, 0.3])
def test_max_rejects_stops_the_load(dialect_session, tmp_path, limit):
    policy = ConversionPolicy(errors=ErrorPolicy(on_error="reject_row", max_rejects=limit))
    with pytest.raises(RejectLimitError):
        _load(dialect_session, tmp_path, _Demo, MESSY, policy=policy)


@pytest.mark.parametrize("limit", [2, 0.5])
def test_max_rejects_allows_a_load_within_the_limit(dialect_session, tmp_path, limit):
    policy = ConversionPolicy(errors=ErrorPolicy(on_error="reject_row", max_rejects=limit))
    assert _load(dialect_session, tmp_path, _Demo, MESSY, policy=policy) == 3


def test_default_policy_raises_on_the_first_bad_batch(dialect_session, tmp_path):
    with pytest.raises(ConversionError, match="demo_number"):
        _load(dialect_session, tmp_path, _Demo, MESSY)
    assert dialect_session.execute(sa.select(sa.func.count()).select_from(DemoRecord)).scalar_one() == 0


def test_report_counts_a_clean_load(dialect_session, tmp_path):
    report = LoadReport()
    _load(dialect_session, tmp_path, _Demo, "demo_id,demo_label\n1,a\n2,b\n", report=report)
    assert (report.rows_read, report.rows_loaded, report.rows_rejected, report.rows_deduplicated) == (2, 2, 0, 0)
    assert report.dedupe is None


def test_dedupe_keeps_the_first_row_and_reports_what_it_removed(dialect_session, tmp_path):
    text = "demo_id,demo_label\n1,first\n2,only\n1,second\n1,third\n3,last\n2,again\n"
    report = LoadReport()
    loaded = _load(dialect_session, tmp_path, _Demo, text, dedupe=True, report=report)
    assert loaded == 3
    assert _labels(dialect_session, DemoRecord) == {1: "first", 2: "only", 3: "last"}
    assert report.dedupe == DedupeResult(("demo_id",), rows_before=6, rows_removed=3)
    assert report.dedupe.rows_after == 3
    assert (report.rows_read, report.rows_loaded, report.rows_deduplicated) == (6, 3, 3)
    assert report.accounted


def test_dedupe_reports_zero_when_nothing_is_duplicated(dialect_session, tmp_path):
    report = LoadReport()
    _load(dialect_session, tmp_path, _Demo, "demo_id,demo_label\n1,a\n2,b\n", dedupe=True, report=report)
    assert report.dedupe == DedupeResult(("demo_id",), rows_before=2, rows_removed=0)


def test_dedupe_works_across_read_batches(sqlite_session, tmp_path):
    rows = ["demo_id,demo_label"] + [f"{i},abcdefghijkl" for i in range(1, 450_001)] + ["1,again", "450000,again"]
    report = LoadReport()
    loaded = _load(sqlite_session, tmp_path, _Demo, "\n".join(rows) + "\n", dedupe=True, report=report)
    assert loaded == 450_000
    assert report.dedupe.rows_removed == 2
    assert report.accounted
    labels = _labels(sqlite_session, DemoRecord)
    assert labels[1] == "abcdefghijkl" and labels[450_000] == "abcdefghijkl"


def test_dedupe_counts_rejected_rows_separately(dialect_session, tmp_path):
    report = LoadReport()
    text = "demo_id,demo_number\n1,1\n1,2\n2,x\n"
    _load(dialect_session, tmp_path, _Demo, text, dedupe=True, report=report, policy=ConversionPolicy(errors=ErrorPolicy(on_error="reject_row")))
    assert (report.rows_read, report.rows_rejected, report.rows_deduplicated, report.rows_loaded) == (3, 1, 1, 1)
    assert report.accounted


def test_parquet_goes_through_the_same_policy(dialect_session, tmp_path):
    table = pa.table({
        "demo_id": pa.array([1, 2, 3, 4], pa.int64()),
        "demo_label": pa.array(["NULL", "x", None, ""], pa.string()),
        "demo_number": pa.array([0, None, 3, 4], pa.int64()),
        "demo_date": pa.array([date(2020, 2, 29), None, date(2020, 1, 1), None], pa.date32()),
    })
    path = tmp_path / "demo_record.parquet"
    pq.write_table(table, path)
    tag = None if dialect_session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA
    _Demo.load_file(dialect_session, path, staging_schema_tag=tag, policy=MARKER)
    dialect_session.commit()
    rows = dialect_session.execute(
        sa.select(DemoRecord.demo_id, DemoRecord.demo_label, DemoRecord.demo_number, DemoRecord.demo_date)
        .order_by(DemoRecord.demo_id)
    ).all()
    assert [tuple(r) for r in rows] == [
        (1, None, 0, date(2020, 2, 29)),
        (2, "x", None, None),
        (3, None, 3, date(2020, 1, 1)),
        (4, None, 4, None),
    ]
