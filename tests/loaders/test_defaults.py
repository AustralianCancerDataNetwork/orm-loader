"""A column with a default takes it when the value is empty or the column is absent."""

from datetime import date, datetime
from pathlib import Path
from typing import Type, cast

import pytest
import sqlalchemy as sa

from orm_loader.backends import STAGING_SCHEMA
from orm_loader.conversion import ConversionPolicy, ErrorPolicy
from orm_loader.helpers import IngestError, RequiredValueError
from orm_loader.tables.typing import FileTableProtocol
from tests.models import DemoDefaultedRecord, DemoDefaultKinds

_Table = cast(Type[FileTableProtocol], DemoDefaultedRecord)
FULL = "demo_id,demo_status,demo_priority,demo_stamp,demo_note\n"


def _load(session, tmp_path: Path, text: str, **kwargs) -> int:
    path = tmp_path / "demo_defaulted_record.csv"
    path.write_text(text, encoding="utf-8")
    tag = None if session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA
    loaded = _Table.load_file(session, path, staging_schema_tag=tag, **kwargs)
    session.commit()
    session.expire_all()
    return loaded


def _rows(session) -> list[tuple]:
    model = DemoDefaultedRecord
    columns = (model.demo_id, model.demo_status, model.demo_priority, model.demo_stamp, model.demo_note)
    return [tuple(r) for r in session.execute(sa.select(*columns).order_by(model.demo_id))]


def test_only_a_default_that_sql_can_apply_makes_a_column_optional():
    assert _Table.required_columns() == {"demo_stamp"}
    assert set(_Table.sql_defaults()) == {"demo_status", "demo_priority", "demo_note"}


def test_an_empty_value_takes_the_default(dialect_session, tmp_path):
    _load(dialect_session, tmp_path, FULL + "1,,,s,\n2,closed,9,s,kept\n")
    assert _rows(dialect_session) == [(1, "open", 5, "s", "n"), (2, "closed", 9, "s", "kept")]


def test_an_absent_column_takes_the_default(dialect_session, tmp_path):
    _load(dialect_session, tmp_path, "demo_id,demo_stamp\n1,s\n2,t\n")
    assert _rows(dialect_session) == [(1, "open", 5, "s", "n"), (2, "open", 5, "t", "n")]


def test_a_column_with_a_callable_default_stays_required(dialect_session, tmp_path):
    with pytest.raises(RequiredValueError, match="demo_stamp"):
        _load(dialect_session, tmp_path, FULL + "1,,,,\n")
    with pytest.raises(IngestError, match="demo_stamp"):
        _load(dialect_session, tmp_path, "demo_id,demo_status\n1,open\n")


def test_a_missing_callable_default_can_be_rejected_by_row(dialect_session, tmp_path):
    policy = ConversionPolicy(errors=ErrorPolicy(on_error="reject_row"))
    _load(dialect_session, tmp_path, FULL + "1,,,,\n2,,,s,\n", policy=policy)
    assert [r[0] for r in _rows(dialect_session)] == [2]


@pytest.mark.parametrize("strategy", ["replace", "upsert"])
def test_a_reload_applies_the_default_to_an_existing_row(dialect_session, tmp_path, strategy):
    _load(dialect_session, tmp_path, FULL + "1,closed,9,s,kept\n")
    _load(dialect_session, tmp_path, FULL + "1,,,t,\n2,,,u,\n", merge_strategy=strategy)
    expected = {"replace": [(1, "open", 5, "t", "n"), (2, "open", 5, "u", "n")],
                "upsert": [(1, "closed", 9, "s", "kept"), (2, "open", 5, "u", "n")]}
    assert _rows(dialect_session) == expected[strategy]


def test_every_kind_of_default_is_applied_and_read_back_on_every_dialect(dialect_session, tmp_path):
    path = tmp_path / "demo_default_kinds.csv"
    path.write_text("kind_id\n1\n2\n")
    tag = None if dialect_session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA
    cast(Type[FileTableProtocol], DemoDefaultKinds).load_file(dialect_session, path, staging_schema_tag=tag)
    dialect_session.commit()
    dialect_session.expire_all()
    rows = dialect_session.execute(sa.select(DemoDefaultKinds)).scalars().all()
    assert len(rows) == 2
    for row in rows:
        assert (row.kind_text, row.kind_int, row.kind_bool) == ("it's text", 7, True)
        assert (row.kind_date, row.kind_stamp) == (date(2000, 1, 2), datetime(2000, 1, 2, 10, 30))
        assert (row.kind_expression, row.kind_py_int, row.kind_py_date) == ("expr", 5, date(2001, 2, 3))
        assert isinstance(row.kind_now, datetime)
