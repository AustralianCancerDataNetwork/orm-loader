"""A merge strategy gives the same result whether or not the target already has rows."""

from pathlib import Path
from typing import Type, cast

import pytest
import sqlalchemy as sa

from oa_configurator import Dialect
from orm_loader.backends import STAGING_SCHEMA
from orm_loader.tables.typing import FileTableProtocol
from tests.models import CompositeTable, SimpleTable

_Simple = cast(Type[FileTableProtocol], SimpleTable)
_Composite = cast(Type[FileTableProtocol], CompositeTable)
DUPLICATED = "id\tname\n1\tfirst\n1\tsecond\n2\tother\n"


@pytest.fixture
def options(dialect_session, request):
    if dialect_session.get_bind().dialect.name == "sqlite":
        return {"staging_schema_tag": None}
    options = {"staging_schema_tag": STAGING_SCHEMA}
    if dialect_session.get_bind().dialect.name == Dialect.DUCKDB:
        options["resolved"] = request.getfixturevalue("duckdb_db").resolved
    return options


def _load(session, tmp_path: Path, text: str, options: dict, **extra) -> None:
    path = tmp_path / "test_table.csv"
    path.write_text(text)
    _Simple.load_file(session, path, dedupe=False, **options, **extra)
    session.commit()


def _rows(session) -> list[tuple]:
    return [tuple(r) for r in session.execute(sa.select(SimpleTable.id, SimpleTable.name).order_by(SimpleTable.id))]


def test_upsert_keeps_the_first_duplicate_in_an_empty_target(dialect_session, tmp_path, options):
    _load(dialect_session, tmp_path, DUPLICATED, options, merge_strategy="upsert")
    assert _rows(dialect_session) == [(1, "first"), (2, "other")]


def test_upsert_keeps_the_first_duplicate_in_a_populated_target(dialect_session, tmp_path, options):
    _load(dialect_session, tmp_path, "id\tname\n9\tseed\n", options, merge_strategy="upsert")
    _load(dialect_session, tmp_path, DUPLICATED, options, merge_strategy="upsert")
    assert _rows(dialect_session) == [(1, "first"), (2, "other"), (9, "seed")]


def test_upsert_into_an_empty_target_hints_at_the_faster_strategy_without_changing_it(
    dialect_session, tmp_path, options, caplog
):
    caplog.set_level("INFO", logger="orm_loader.tables.loadable_table")
    _load(dialect_session, tmp_path, DUPLICATED, options, merge_strategy="upsert")
    messages = [record.getMessage() for record in caplog.records]
    assert any("`insert` with `dedupe=True` loads the same rows faster" in m for m in messages)
    assert any("Merge upsert phase starting." in m for m in messages)
    assert not any("Merge insert phase starting." in m for m in messages)


def test_replace_with_a_composite_key_overwrites_matching_rows(dialect_session, tmp_path, options):
    path = tmp_path / "composite_table.csv"
    for text in ("a\tb\tvalue\n1\t1\told\n1\t2\tkept\n", "a\tb\tvalue\n1\t1\tnew\n2\t2\tadded\n"):
        path.write_text(text)
        _Composite.load_file(dialect_session, path, dedupe=False, merge_strategy="replace", **options)
        dialect_session.commit()
    rows = dialect_session.execute(
        sa.select(CompositeTable.a, CompositeTable.b, CompositeTable.value).order_by(CompositeTable.a, CompositeTable.b)
    )
    assert [tuple(r) for r in rows] == [(1, 1, "new"), (1, 2, "kept"), (2, 2, "added")]


def test_insert_fails_on_an_existing_key_and_adds_new_ones(dialect_session, tmp_path, options):
    _load(dialect_session, tmp_path, "id\tname\n1\ta\n", options, merge_strategy="insert")
    _load(dialect_session, tmp_path, "id\tname\n2\tb\n", options, merge_strategy="insert")
    assert _rows(dialect_session) == [(1, "a"), (2, "b")]
    with pytest.raises(sa.exc.IntegrityError):
        _load(dialect_session, tmp_path, "id\tname\n2\tagain\n", options, merge_strategy="insert")
