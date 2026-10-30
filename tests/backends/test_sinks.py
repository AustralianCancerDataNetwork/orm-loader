"""The sink contract: every backend writes the same typed batches to staging the same way."""

from datetime import date
from typing import Type, cast

import pyarrow as pa
import pytest
import sqlalchemy as sa

from orm_loader.backends import STAGING_SCHEMA, resolve_backend
from orm_loader.loaders import LoaderContext
from orm_loader.tables.typing import FileTableProtocol
from tests.models import DemoRecord

_Demo = cast(Type[FileTableProtocol], DemoRecord)
COLUMNS = ["demo_id", "demo_label", "demo_number", "demo_flag", "demo_date"]


def _sink(session, tmp_path):
    tag = None if session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA
    resolve_backend(session, staging_schema_tag=tag, mapper=_Demo).create_staging_table(_Demo, session)
    staging = _Demo.get_staging_table(session, staging_schema_tag=tag)
    ctx = LoaderContext(tableclass=_Demo, session=session, path=tmp_path / "demo_record.csv", staging_schema_tag=tag)
    return resolve_backend(session, staging_schema_tag=tag, mapper=_Demo), ctx, staging


def _batch(i: int, label: str | None) -> pa.Table:
    return pa.table({
        "demo_id": [i],
        "demo_label": pa.array([label], pa.string()),
        "demo_number": [i * 10],
        "demo_flag": [i % 2 == 0],
        "demo_date": pa.array([date(2020, 1, i)], pa.date32()),
    })


def _stored(session, staging):
    """Return the staged rows in insertion order (SQLite numbers rows with its implicit rowid)."""
    order = staging.c._rownum if "_rownum" in staging.c else sa.literal_column("rowid")
    return session.execute(sa.select(staging).order_by(order)).all()


def test_typed_values_and_nulls_are_stored_across_more_batches_than_a_commit_interval(dialect_session, tmp_path):
    backend, ctx, staging = _sink(dialect_session, tmp_path)
    batches = [_batch(i, f"row {i}" if i % 2 else None) for i in range(1, 13)]
    backend.insert_batches(ctx, staging, batches, COLUMNS)
    stored = _stored(dialect_session, staging)
    assert [r.demo_id for r in stored] == list(range(1, 13))
    assert stored[0].demo_label == "row 1" and stored[1].demo_label is None
    assert stored[1].demo_flag is True and stored[1].demo_date == date(2020, 1, 2)
    assert stored[2].demo_number == 30


def test_rows_keep_the_order_they_were_written_in(dialect_session, tmp_path):
    backend, ctx, staging = _sink(dialect_session, tmp_path)
    ids = [5, 3, 9, 1, 7]
    backend.insert_batches(ctx, staging, [_batch(i, None) for i in ids], COLUMNS)
    assert [r.demo_id for r in _stored(dialect_session, staging)] == ids


def test_no_batches_write_nothing(dialect_session, tmp_path):
    backend, ctx, staging = _sink(dialect_session, tmp_path)
    backend.insert_batches(ctx, staging, [], COLUMNS)
    assert _stored(dialect_session, staging) == []


@pytest.mark.parametrize(
    "label", ["", "a,b", 'say "hi"', "line\nbreak", "tab\there", "ünïcode ✓", "NULL", "\\N"]
)
def test_text_is_stored_verbatim_and_an_empty_string_stays_distinct_from_null(dialect_session, tmp_path, label):
    backend, ctx, staging = _sink(dialect_session, tmp_path)
    backend.insert_batches(ctx, staging, [_batch(1, label), _batch(2, None)], COLUMNS)
    stored = _stored(dialect_session, staging)
    assert (stored[0].demo_label, stored[1].demo_label) == (label, None)


def test_a_failing_batch_source_is_raised_not_swallowed(dialect_session, tmp_path):
    backend, ctx, staging = _sink(dialect_session, tmp_path)

    def batches():
        yield _batch(1, "a")
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        backend.insert_batches(ctx, staging, batches(), COLUMNS)


def _staging_for(session, table):
    tag = None if session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA
    resolve_backend(session, staging_schema_tag=tag, mapper=table).create_staging_table(table, session)
    return table.get_staging_table(session, staging_schema_tag=tag)


def test_a_staging_table_mirrors_the_columns_and_nullability_of_the_target(dialect_session):
    from tests.models import DemoRequiredRecord

    staging = _staging_for(dialect_session, cast(Type[FileTableProtocol], DemoRequiredRecord))
    target = DemoRequiredRecord.__table__
    assert [c.name for c in staging.c if c.name != "_rownum"] == [c.name for c in target.c]
    assert {c.name: c.nullable for c in staging.c if c.name != "_rownum"} == {c.name: c.nullable for c in target.c}
    assert not target.c.demo_label.nullable


def test_a_staging_table_has_no_key_and_no_default(dialect_session):
    from tests.models import DemoDefaultedRecord

    staging = _staging_for(dialect_session, cast(Type[FileTableProtocol], DemoDefaultedRecord))
    assert staging.primary_key.columns.keys() == []
    assert all(c.server_default is None for c in staging.c if c.name != "_rownum")


def test_a_staging_table_can_be_dropped_twice_and_created_again(dialect_session):
    staging = _staging_for(dialect_session, _Demo)
    tag = None if dialect_session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA
    for _ in range(2):
        resolve_backend(dialect_session, staging_schema_tag=tag, mapper=_Demo).drop_staging_table(_Demo, dialect_session)
    with pytest.raises(sa.exc.NoSuchTableError):
        _Demo.get_staging_table(dialect_session, staging_schema_tag=tag)
    assert _staging_for(dialect_session, _Demo).name == staging.name
