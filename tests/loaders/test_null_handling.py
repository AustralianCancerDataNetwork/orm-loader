"""Null handling: nonempty marker-like text is preserved verbatim.

Typed columns accept a valid value or a recognised missing word and raise
otherwise. Empty and whitespace-only values are missing.
"""

from datetime import date
from pathlib import Path
from typing import Type, cast

import pyarrow as pa
import pytest
import sqlalchemy as sa
from oa_configurator.testing import DIALECT_PARAMS

from orm_loader.backends import STAGING_SCHEMA, Dialect
from orm_loader.helpers import IngestError
from orm_loader.loaders.data.converters import cast_arrow_column, cast_scalar
from orm_loader.tables.typing import CSVTableProtocol
from tests.models import DemoRecord, DemoRequiredRecord

_Demo = cast(Type[CSVTableProtocol], DemoRecord)
_DemoRequired = cast(Type[CSVTableProtocol], DemoRequiredRecord)

MARKER_WORDS = [
    "Null", "NULL", "null", "None", "none", "NA", "N/A", "NaN", "nan",
    "#N/A", "<NA>", "Contains NULL here", '"NULL"',
]

SEVEN_ROWS = 'demo_id,demo_label\n1,NULL\n2,"NULL"\n3,"""NULL"""\n4,\n5,""\n6,"alpha,beta"\n7,"alpha\nbeta"\n'
SEVEN_ROWS_QUOTED = (
    '"demo_id","demo_label"\n"1","NULL"\n"2","NULL"\n"3","""NULL"""\n"4",""\n"5",""\n'
    '"6","alpha,beta"\n"7","alpha\nbeta"\n'
)
SEVEN_EXPECTED = {
    1: "NULL", 2: "NULL", 3: '"NULL"', 4: None, 5: None, 6: "alpha,beta", 7: "alpha\nbeta",
}


_SESSION_FIXTURE_BY_DIALECT = {
    Dialect.SQLITE: "sqlite_session",
    Dialect.POSTGRESQL: "pg_session",
    Dialect.DUCKDB: "duckdb_session",
}


@pytest.fixture(params=DIALECT_PARAMS)
def session(request):
    return request.getfixturevalue(_SESSION_FIXTURE_BY_DIALECT[request.param])


def _write(tmp_path: Path, table: type, text: str) -> Path:
    path = tmp_path / f"{table.__tablename__}.csv"
    path.write_text(text, encoding="utf-8", newline="")
    return path


def _load(session, tmp_path: Path, table, text: str, **kwargs) -> int:
    tag = None if session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA
    n = table.load_csv(session, _write(tmp_path, table, text), staging_schema_tag=tag, **kwargs)
    session.commit()
    session.expire_all()
    return n


def _labels(session, model) -> dict[int, str | None]:
    rows = session.execute(sa.select(model.demo_id, model.demo_label).order_by(model.demo_id)).all()
    return {r[0]: r[1] for r in rows}


# Marker-like text through each entry point

@pytest.mark.parametrize("word", MARKER_WORDS)
def test_scalar_text_keeps_marker_words(word):
    assert cast_scalar(word, sa.String()) == word


@pytest.mark.parametrize("word", MARKER_WORDS)
def test_arrow_text_keeps_marker_words(word):
    out = cast_arrow_column(pa.array([word, "plain"]), DemoRecord.__table__.c.demo_label)
    assert out.to_pylist() == [word, "plain"]


@pytest.mark.parametrize("word", MARKER_WORDS)
def test_dict_path_keeps_marker_words(word):
    obj = DemoRecord(**DemoRecord.clean_kwargs({"demo_id": 1, "demo_label": word}))
    assert obj.demo_label == word


# Actual missing objects

@pytest.mark.parametrize("value", [None, float("nan"), "", "   "])
def test_scalar_missing_objects_are_null(value):
    assert cast_scalar(value, sa.String()) is None


def test_arrow_null_bitmap_and_nan_are_null():
    col = DemoRecord.__table__.c.demo_number
    assert cast_arrow_column(pa.array([1, None, 0], pa.int64()), col).to_pylist() == [1, None, 0]
    assert cast_arrow_column(pa.array([1.0, float("nan")], pa.float64()), col).to_pylist() == [1, None]


# Whitespace and identifiers

def test_text_is_stripped_by_default():
    assert cast_scalar("  x  ", sa.String()) == "x"


@pytest.mark.parametrize("code", ["007", "0123456", "1.50", "+5", "12.0"])
def test_text_is_never_canonicalised_as_a_number(code):
    assert cast_scalar(code, sa.String()) == code
    assert cast_arrow_column(pa.array([code]), DemoRecord.__table__.c.demo_label).to_pylist() == [code]


# Typed columns: valid value, missing word, or error

@pytest.mark.parametrize("word", ["null", "None", "NA", "N/A", "NaN", "#N/A"])
def test_typed_missing_words_are_null(word):
    assert cast_scalar(word, sa.Integer()) is None
    assert cast_scalar(word, sa.Date()) is None


@pytest.mark.parametrize("word", ["null", "None", "NA", "N/A", "NaN"])
def test_typed_float_missing_words_are_null(word):
    assert cast_scalar(word, sa.Float()) is None


# Persisted round trips

def test_seven_row_fixture_preserves_text(session, tmp_path):
    _load(session, tmp_path, _Demo, SEVEN_ROWS)
    assert _labels(session, DemoRecord) == SEVEN_EXPECTED


def test_seven_row_fixture_every_field_quoted(session, tmp_path):
    _load(session, tmp_path, _Demo, SEVEN_ROWS_QUOTED)
    assert _labels(session, DemoRecord) == SEVEN_EXPECTED


def test_marker_words_survive_tab_delimited_load(session, tmp_path):
    words = [w for w in MARKER_WORDS if w != '"NULL"']
    text = "demo_id\tdemo_label\n" + "".join(f"{i}\t{w}\n" for i, w in enumerate(words, 1))
    _load(session, tmp_path, _Demo, text)
    assert _labels(session, DemoRecord) == dict(enumerate(words, 1))


def test_literal_quotes_survive_tab_delimited_load(session, tmp_path):
    values = ['"NULL"', '"word" label', "Null", "NA"]
    text = "demo_id\tdemo_label\n" + "".join(f"{i}\t{v}\n" for i, v in enumerate(values, 1)) + "5\t\n"
    _load(session, tmp_path, _Demo, text, quote_mode="literal")
    assert _labels(session, DemoRecord) == {**dict(enumerate(values, 1)), 5: None}


def test_typed_values_round_trip(session, tmp_path):
    text = (
        "demo_id,demo_label,demo_number,demo_flag,demo_date\n"
        "1,a,0,false,20200229\n"
        "2,b,NA,,\n"
        "3,c,7,true,2020-01-02\n"
    )
    _load(session, tmp_path, _Demo, text)
    rows = session.execute(sa.select(
        DemoRecord.demo_id, DemoRecord.demo_number, DemoRecord.demo_flag, DemoRecord.demo_date
    ).order_by(DemoRecord.demo_id)).all()
    assert [tuple(r) for r in rows] == [
        (1, 0, False, date(2020, 2, 29)),
        (2, None, None, None),
        (3, 7, True, date(2020, 1, 2)),
    ]


@pytest.mark.parametrize(
    "column, bad",
    [
        ("demo_date", "20200231"),
        ("demo_date", "Jan 2 2020"),
        ("demo_date", "02/01/2020"),
        ("demo_number", "12abc"),
        ("demo_number", "0x1F"),
        ("demo_number", "1.5"),
    ],
)
def test_unreadable_typed_value_raises_and_loads_nothing(session, tmp_path, column, bad):
    cols = {"demo_number": "", "demo_date": ""}
    cols[column] = bad
    text = f"demo_id,demo_label,demo_number,demo_date\n1,a,,\n2,b,{cols['demo_number']},{cols['demo_date']}\n"
    with pytest.raises(IngestError, match=column):
        _load(session, tmp_path, _Demo, text)
    session.rollback()
    assert session.execute(sa.select(sa.func.count()).select_from(DemoRecord)).scalar_one() == 0


# Required columns

def test_required_text_accepts_marker_words(session, tmp_path):
    words = ["Null", "None", "NA", "N/A", "NaN"]
    text = "demo_id,demo_label\n" + "".join(f"{i},{w}\n" for i, w in enumerate(words, 1))
    _load(session, tmp_path, _DemoRequired, text)
    assert _labels(session, DemoRequiredRecord) == dict(enumerate(words, 1))


@pytest.mark.parametrize("missing", ["", '""', "   "])
def test_required_text_empty_raises_and_loads_nothing(session, tmp_path, missing):
    text = f"demo_id,demo_label\n1,ok\n2,{missing}\n"
    with pytest.raises(IngestError, match="demo_label"):
        _load(session, tmp_path, _DemoRequired, text)
    session.rollback()
    assert session.execute(sa.select(sa.func.count()).select_from(DemoRequiredRecord)).scalar_one() == 0


def test_required_empty_raises_atomically_with_batching(session, tmp_path):
    text = "demo_id,demo_label\n1,a\n2,b\n3,\n"
    with pytest.raises(IngestError, match="demo_label"):
        _load(session, tmp_path, _DemoRequired, text, merge_batch_size=1)
    session.rollback()
    assert session.execute(sa.select(sa.func.count()).select_from(DemoRequiredRecord)).scalar_one() == 0
