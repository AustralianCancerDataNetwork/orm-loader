"""Input that already has a type, such as a Parquet column, is cast and checked like parsed text."""

from datetime import date, datetime, timezone
from decimal import Decimal

import pyarrow as pa
import sqlalchemy as sa

from tests.support import interpret


def _result(values: pa.Array, sa_type):
    return interpret(values, sa_type)


def test_a_boolean_column_accepts_only_zero_and_one_from_integers():
    values, invalid = _result(pa.array([0, 1, 2, None]), sa.Boolean())
    assert values == [False, True, None, None]
    assert invalid == [False, False, True, False]


def test_a_date_column_accepts_only_midnight_timestamps():
    midnight, noon = datetime(2020, 5, 17), datetime(2020, 5, 17, 12)
    values, invalid = _result(pa.array([midnight, noon], pa.timestamp("us")), sa.Date())
    assert values == [date(2020, 5, 17), None]
    assert invalid == [False, True]


def test_the_length_of_text_cast_from_another_type_is_checked():
    values, invalid = _result(pa.array([123, 123456]), sa.String(5))
    assert values == ["123", None]
    assert invalid == [False, True]


def test_integer_width_is_checked_on_typed_input():
    _, invalid = _result(pa.array([70_000, 1], pa.int64()), sa.SmallInteger())
    assert invalid == [True, False]


def test_floats_cannot_be_truncated_to_integers():
    values, invalid = _result(pa.array([1.0, 1.5]), sa.Integer())
    assert values == [1, None]
    assert invalid == [False, True]


def test_float_nan_is_missing_and_infinity_is_unreadable():
    values, invalid = _result(pa.array([float("nan"), float("inf"), 2.5]), sa.Float())
    assert values == [None, None, 2.5]
    assert invalid == [False, True, False]


def test_decimals_that_lose_digits_are_unreadable():
    values, invalid = _result(pa.array([Decimal("1.25"), Decimal("1.234")], pa.decimal128(10, 3)), sa.Numeric(10, 2))
    assert values == [Decimal("1.25"), None]
    assert invalid == [False, True]


def test_an_all_null_column_of_unknown_type_is_missing_values():
    values, invalid = _result(pa.array([None, None]), sa.Integer())
    assert values == [None, None]
    assert invalid == [False, False]


def test_timestamps_with_and_without_a_time_zone_match_their_column():
    aware = pa.array([datetime(2020, 1, 1, 10, tzinfo=timezone.utc)], pa.timestamp("us", "UTC"))
    naive = pa.array([datetime(2020, 1, 1, 10)], pa.timestamp("us"))
    assert _result(aware, sa.DateTime(timezone=True)) == ([datetime(2020, 1, 1, 10, tzinfo=timezone.utc)], [False])
    assert _result(naive, sa.DateTime()) == ([datetime(2020, 1, 1, 10)], [False])


def test_year_zero_dates_are_unreadable_on_typed_input():
    year_zero, year_two_thousand = -719_528, 10_957
    _, invalid = _result(pa.array([year_zero, year_two_thousand], pa.date32()), sa.Date())
    assert invalid == [True, False]
