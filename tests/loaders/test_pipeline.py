"""The whole pipeline, file to staging table, on every dialect and in every file format."""

import csv
import io
from datetime import date
from pathlib import Path
from typing import Type, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa

from orm_loader.backends import STAGING_SCHEMA
from orm_loader.conversion import ConversionPolicy, ErrorPolicy
from orm_loader.helpers import ConversionError, RequiredValueError
from orm_loader.loaders import DelimitedTextReader, LoaderContext, ParquetReader, stage_file
from orm_loader.tables.typing import FileTableProtocol
from tests.models import DemoRecord, DemoRequiredRecord

_Demo = cast(Type[FileTableProtocol], DemoRecord)
_DemoRequired = cast(Type[FileTableProtocol], DemoRequiredRecord)

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
    ctx = LoaderContext(
        tableclass=table, session=session, path=path, staging_table=staging, staging_schema_tag=tag, **context
    )
    loaded = stage_file(ctx, reader or table._select_reader(ctx))
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
    reader = _Demo._select_reader(
        LoaderContext(tableclass=_Demo, session=dialect_session, path=path, staging_table=cast(sa.Table, DemoRecord.__table__))
    )

    def broken():
        raise RuntimeError("boom")
        yield  # pragma: no cover

    reader.batches = broken  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="boom"):
        _stage(dialect_session, _Demo, path, reader=reader)
    tag = None if dialect_session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA
    assert _staged(dialect_session, _Demo.get_staging_table(dialect_session, staging_schema_tag=tag), "demo_id") == []
