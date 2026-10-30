"""The whole pipeline, file to staging table, on every dialect and in every file format."""

import csv
import io
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Type, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa

from orm_loader.backends import STAGING_SCHEMA
from orm_loader.conversion import ConversionPolicy, ErrorPolicy
from orm_loader.helpers import ConversionError, RequiredValueError
from orm_loader.loaders import DelimitedTextReader, LoaderContext, ParquetReader, open_reader, stage_file
from orm_loader.tables.typing import FileTableProtocol
from tests.models import DemoRecord, DemoRequiredRecord, DemoTypedRecord

_Demo = cast(Type[FileTableProtocol], DemoRecord)
_DemoRequired = cast(Type[FileTableProtocol], DemoRequiredRecord)
_Typed = cast(Type[FileTableProtocol], DemoTypedRecord)

HEADER = ["demo_id", "demo_label", "demo_number", "demo_flag", "demo_date"]
REJECT = ConversionPolicy(errors=ErrorPolicy(on_error="reject_row"))


@pytest.fixture(params=["csv", "tsv", "parquet"])
def fmt(request) -> str:
    return request.param


def _write(tmp_path: Path, table, fmt: str, rows: list[tuple], header: list[str] = HEADER) -> Path:
    """Write the same rows as comma or tab delimited text, or as Parquet text columns."""
    path = tmp_path / f"{table.__tablename__}.{'parquet' if fmt == 'parquet' else fmt}"
    if fmt == "parquet":
        pq.write_table(pa.table({name: pa.array([r[i] for r in rows], pa.string()) for i, name in enumerate(header)}), path)
    else:
        buffer = io.StringIO()
        csv.writer(buffer, delimiter="," if fmt == "csv" else "\t", lineterminator="\n").writerows([header, *rows])
        path.write_text(buffer.getvalue(), encoding="utf-8", newline="")
    return path


def _stage(session, table, path: Path, reader=None, **context):
    tag = None if session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA
    table.create_staging_table(session, staging_schema_tag=tag)
    staging = table.get_staging_table(session, staging_schema_tag=tag)
    ctx = LoaderContext(tableclass=table, session=session, path=path, staging_schema_tag=tag, **context)
    loaded = stage_file(ctx, reader or open_reader(path, table.model_columns(), nulls=ctx.policy.nulls), staging)
    return loaded, ctx, staging


def _staged(session, staging, *columns: str) -> list[tuple]:
    order = staging.c._rownum if "_rownum" in staging.c else sa.literal_column("rowid")
    return [tuple(r) for r in session.execute(sa.select(*(staging.c[c] for c in columns)).order_by(order)).all()]


def test_rows_are_converted_to_the_column_types(dialect_session, tmp_path, fmt):
    rows = [("1", "alpha", "10", "true", "20200229"), ("2", " beta ", "", "no", "24-AUG-2017")]
    loaded, ctx, staging = _stage(dialect_session, _Demo, _write(tmp_path, DemoRecord, fmt, rows))
    assert loaded == 2 and ctx.report.accounted
    assert _staged(dialect_session, staging, *HEADER) == [
        (1, "alpha", 10, True, date(2020, 2, 29)),
        (2, "beta", None, False, date(2017, 8, 24)),
    ]


def test_marker_words_survive_and_only_blanks_are_null(dialect_session, tmp_path, fmt):
    labels = ["NULL", "None", "N/A", "NaN", "", "  ", " padded "]
    rows = [(str(i), label, "", "", "") for i, label in enumerate(labels, start=1)]
    _, _, staging = _stage(dialect_session, _Demo, _write(tmp_path, DemoRecord, fmt, rows))
    assert [r[0] for r in _staged(dialect_session, staging, "demo_label")] == [
        "NULL", "None", "N/A", "NaN", None, None, "padded",
    ]


def test_missing_words_in_typed_columns_are_null(dialect_session, tmp_path, fmt):
    rows = [("1", "x", "NA", "null", "N/A")]
    _, _, staging = _stage(dialect_session, _Demo, _write(tmp_path, DemoRecord, fmt, rows))
    assert _staged(dialect_session, staging, "demo_number", "demo_flag", "demo_date") == [(None, None, None)]


def test_an_unreadable_value_raises_and_stages_nothing(dialect_session, tmp_path, fmt):
    rows = [("1", "a", "1", "", ""), ("2", "b", "abc", "", "")]
    path = _write(tmp_path, DemoRecord, fmt, rows)
    with pytest.raises(ConversionError, match="demo_number"):
        _stage(dialect_session, _Demo, path)
    staging = _Demo.get_staging_table(dialect_session, staging_schema_tag=None if dialect_session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA)
    assert _staged(dialect_session, staging, "demo_id") == []


def test_a_missing_required_value_raises(dialect_session, tmp_path, fmt):
    path = _write(tmp_path, DemoRequiredRecord, fmt, [("1", "ok"), ("2", "")], ["demo_id", "demo_label"])
    with pytest.raises(RequiredValueError):
        _stage(dialect_session, _DemoRequired, path)


def test_reject_row_loads_the_rest_and_accounts_for_every_row(dialect_session, tmp_path, fmt):
    rows = [("1", "a", "1", "", ""), ("2", "b", "abc", "", ""), ("3", "c", "3", "", "02/01/2020"), ("4", "d", "", "", "")]
    loaded, ctx, staging = _stage(
        dialect_session, _Demo, _write(tmp_path, DemoRecord, fmt, rows), policy=REJECT, rejects_dir=tmp_path / "rej"
    )
    assert loaded == 2
    assert (ctx.report.rows_read, ctx.report.rows_loaded, ctx.report.rows_rejected) == (4, 2, 2)
    assert ctx.report.accounted
    assert [r[0] for r in _staged(dialect_session, staging, "demo_id")] == [1, 4]
    assert ctx.report.rejects_path is not None and ctx.report.rejects_path.exists()


def test_dedupe_keeps_the_first_row_for_each_key(dialect_session, tmp_path, fmt):
    rows = [("1", "first", "", "", ""), ("2", "x", "", "", ""), ("1", "second", "", "", "")]
    loaded, ctx, staging = _stage(dialect_session, _Demo, _write(tmp_path, DemoRecord, fmt, rows), dedupe=True)
    assert loaded == 2 and ctx.report.rows_deduplicated == 1 and ctx.report.accounted
    assert sorted(_staged(dialect_session, staging, "demo_id", "demo_label")) == [(1, "first"), (2, "x")]


def test_a_file_in_many_batches_loads_every_row_in_order(dialect_session, tmp_path, fmt):
    rows = [(str(i), f"label {i}", str(i), "", "") for i in range(1, 1501)]
    path = _write(tmp_path, DemoRecord, fmt, rows)
    columns = DemoRecord.model_columns()
    reader = ParquetReader(path, columns, chunksize=100) if fmt == "parquet" else DelimitedTextReader(path, columns, block_size=1 << 10)
    loaded, ctx, staging = _stage(dialect_session, _Demo, path, reader=reader)
    assert loaded == 1500 and ctx.report.accounted
    assert [r[0] for r in _staged(dialect_session, staging, "demo_id")] == list(range(1, 1501))


def test_a_failing_reader_is_raised_and_stages_nothing(dialect_session, tmp_path, fmt):
    path = _write(tmp_path, DemoRecord, fmt, [("1", "a", "", "", "")])
    reader = open_reader(path, _Demo.model_columns())

    def broken():
        raise RuntimeError("boom")
        yield  # pragma: no cover

    reader.batches = broken  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="boom"):
        _stage(dialect_session, _Demo, path, reader=reader)
    tag = None if dialect_session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA
    assert _staged(dialect_session, _Demo.get_staging_table(dialect_session, staging_schema_tag=tag), "demo_id") == []


TYPED_HEADER = [
    "typed_id", "typed_small", "typed_float", "typed_numeric", "typed_flag",
    "typed_date", "typed_stamp", "typed_stamp_utc", "typed_label",
]
UTC = timezone.utc


def _typed_file(tmp_path: Path, **overrides: pa.Array) -> Path:
    columns = {
        "typed_id": pa.array([1, 2], pa.int64()),
        "typed_small": pa.array([7, None], pa.int32()),
        "typed_float": pa.array([1.5, float("nan")], pa.float32()),
        "typed_numeric": pa.array([Decimal("12.34"), None], pa.decimal128(10, 2)),
        "typed_flag": pa.array([True, None], pa.bool_()),
        "typed_date": pa.array([date(2020, 2, 29), None], pa.date32()),
        "typed_stamp": pa.array([datetime(2020, 1, 1, 10, 30), None], pa.timestamp("us")),
        "typed_stamp_utc": pa.array([datetime(2020, 1, 1, 10, 30, tzinfo=UTC), None], pa.timestamp("us", "UTC")),
        "typed_label": pa.array(["alpha", None], pa.string()),
        **overrides,
    }
    path = tmp_path / "demo_typed_record.parquet"
    pq.write_table(pa.table(columns), path)
    return path


def test_typed_parquet_columns_keep_their_values_on_every_dialect(dialect_session, tmp_path):
    loaded, ctx, staging = _stage(dialect_session, _Typed, _typed_file(tmp_path))
    assert loaded == 2 and ctx.report.accounted
    rows = _staged(dialect_session, staging, *TYPED_HEADER)
    assert rows[0] == (
        1, 7, 1.5, Decimal("12.34"), True, date(2020, 2, 29),
        datetime(2020, 1, 1, 10, 30), datetime(2020, 1, 1, 10, 30, tzinfo=UTC), "alpha",
    )
    assert rows[1] == (2, None, None, None, None, None, None, None, None)


@pytest.mark.parametrize(
    "column, bad",
    [
        ("typed_flag", pa.array([1, 2], pa.int64())),
        ("typed_date", pa.array([datetime(2020, 1, 1), datetime(2020, 1, 1, 12)], pa.timestamp("us"))),
        ("typed_small", pa.array([1, 70_000], pa.int64())),
        ("typed_label", pa.array([1, 12345678901], pa.int64())),
        ("typed_numeric", pa.array([Decimal("1.25"), Decimal("1.234")], pa.decimal128(10, 3))),
    ],
)
def test_typed_parquet_values_that_would_change_are_unreadable(dialect_session, tmp_path, column, bad):
    with pytest.raises(ConversionError, match=column):
        _stage(dialect_session, _Typed, _typed_file(tmp_path, **{column: bad}))
    loaded, ctx, _ = _stage(dialect_session, _Typed, _typed_file(tmp_path, **{column: bad}), policy=REJECT)
    assert loaded == 1 and ctx.report.rows_rejected == 1 and ctx.report.accounted


def _typed_rows(**cells: str) -> list[tuple]:
    return [tuple(cells.get(name, "1" if name == "typed_id" else "") for name in TYPED_HEADER)]


def test_offsets_are_stored_as_utc_in_a_time_zone_column_and_rejected_in_a_naive_one(dialect_session, tmp_path):
    rows = _typed_rows(typed_stamp_utc="2020-01-01T10:00:00+10:00", typed_stamp="2020-01-01 10:00:00")
    _, _, staging = _stage(dialect_session, _Typed, _write(tmp_path, DemoTypedRecord, "csv", rows, TYPED_HEADER))
    assert _staged(dialect_session, staging, "typed_stamp", "typed_stamp_utc") == [
        (datetime(2020, 1, 1, 10), datetime(2020, 1, 1, 0, tzinfo=UTC))
    ]
    with pytest.raises(ConversionError, match="typed_stamp.*without an offset"):
        bad = _typed_rows(typed_stamp="2020-01-01T10:00:00+10:00")
        _stage(dialect_session, _Typed, _write(tmp_path, DemoTypedRecord, "csv", bad, TYPED_HEADER))
    with pytest.raises(ConversionError, match="typed_stamp_utc.*with an offset"):
        bad = _typed_rows(typed_stamp_utc="2020-01-01 10:00:00")
        _stage(dialect_session, _Typed, _write(tmp_path, DemoTypedRecord, "csv", bad, TYPED_HEADER))


@pytest.mark.parametrize("policy", [None, REJECT], ids=["raise", "reject_row"])
def test_an_integer_too_wide_for_the_column_never_reaches_the_database(dialect_session, tmp_path, policy):
    rows = [*_typed_rows(typed_small="32767"), *_typed_rows(typed_id="2", typed_small="32768")]
    path = _write(tmp_path, DemoTypedRecord, "csv", rows, TYPED_HEADER)
    if policy is None:
        with pytest.raises(ConversionError, match="typed_small"):
            _stage(dialect_session, _Typed, path)
        return
    loaded, ctx, staging = _stage(dialect_session, _Typed, path, policy=policy)
    assert loaded == 1 and ctx.report.rows_rejected == 1
    assert _staged(dialect_session, staging, "typed_small") == [(32767,)]


def test_year_zero_is_rejected_on_every_dialect(dialect_session, tmp_path):
    rows = _typed_rows(typed_date="0000-01-01", typed_stamp="0000-01-01 00:00:00")
    with pytest.raises(ConversionError, match="typed_date"):
        _stage(dialect_session, _Typed, _write(tmp_path, DemoTypedRecord, "csv", rows, TYPED_HEADER))
