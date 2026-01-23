import sqlalchemy as sa
from orm_loader.loaders.data.converters import perform_cast
from datetime import date, datetime

def test_perform_cast_integer():
    assert perform_cast("123", sa.Integer(), on_error=None) == 123

def test_perform_cast_invalid_integer_returns_none():
    errors = []
    def on_error(v): errors.append(v)

    result = perform_cast("abc", sa.Integer(), on_error=on_error)

    assert result is None
    assert errors == ["abc"]

def test_perform_cast_boolean():
    assert perform_cast("yes", sa.Boolean(), on_error=None) is True
    assert perform_cast("no", sa.Boolean(), on_error=None) is False

def test_integer_from_numeric_string():
    assert perform_cast("42", sa.Integer(), on_error=None) == 42

def test_integer_from_float_string():
    assert perform_cast("42.0", sa.Integer(), on_error=None) == 42

def test_integer_from_actual_float():
    assert perform_cast(42.0, sa.Integer(), on_error=None) == 42

def test_integer_whitespace():
    assert perform_cast("  7  ", sa.Integer(), on_error=None) == 7

def test_integer_invalid_string_returns_none_and_records_error():
    errors = []
    result = perform_cast("Episode", sa.Integer(), on_error=errors.append)
    assert result is None
    assert errors == ["Episode"]

def test_integer_empty_string_is_null_not_error():
    errors = []
    assert perform_cast("", sa.Integer(), on_error=errors.append) is None
    assert errors == []

def test_date_from_date():
    d = date(2020, 5, 17)
    assert perform_cast(d, sa.Date(), on_error=None) == d

def test_date_from_datetime():
    dt = datetime(2020, 5, 17, 14, 30)
    assert perform_cast(dt, sa.Date(), on_error=None) == date(2020, 5, 17)

def test_date_yyyymmdd():
    assert perform_cast("20170824", sa.Date(), on_error=None) == date(2017, 8, 24)

def test_date_dd_mmm_yyyy():
    assert perform_cast("24-AUG-2017", sa.Date(), on_error=None) == date(2017, 8, 24)

def test_date_iso():
    assert perform_cast("2017-08-24", sa.Date(), on_error=None) == date(2017, 8, 24)

def test_date_dd_mm_yyyy():
    assert perform_cast("24/08/2017", sa.Date(), on_error=None) == date(2017, 8, 24)

def test_date_rejects_iso_datetime():
    assert perform_cast("2017-08-24T12:00:00", sa.Date(), on_error=None) is None

def test_date_rejects_invalid_date():
    assert perform_cast("2017-99-99", sa.Date(), on_error=None) is None

def test_date_rejects_fuzzy():
    assert perform_cast("Aug 24 2017", sa.Date(), on_error=None) is None

def test_datetime_from_datetime():
    dt = datetime(2020, 5, 17, 14, 30, 5)
    assert perform_cast(dt, sa.DateTime(), on_error=None) == dt

def test_datetime_from_date():
    d = date(2020, 5, 17)
    assert perform_cast(d, sa.DateTime(), on_error=None) == datetime(2020, 5, 17, 0, 0)

def test_datetime_iso_basic():
    assert perform_cast(
        "2017-08-24T12:34:56",
        sa.DateTime(),
        on_error=None,
    ) == datetime(2017, 8, 24, 12, 34, 56)

def test_datetime_iso_with_seconds_only():
    assert perform_cast(
        "2017-08-24T00:00:00",
        sa.DateTime(),
        on_error=None,
    ) == datetime(2017, 8, 24, 0, 0, 0)


def test_datetime_from_yyyymmdd():
    assert perform_cast(
        "20170824",
        sa.DateTime(),
        on_error=None,
    ) == datetime(2017, 8, 24, 0, 0)

def test_datetime_from_dd_mmm_yyyy():
    assert perform_cast(
        "24-AUG-2017",
        sa.DateTime(),
        on_error=None,
    ) == datetime(2017, 8, 24, 0, 0)

def test_datetime_from_iso_date():
    assert perform_cast(
        "2017-08-24",
        sa.DateTime(),
        on_error=None,
    ) == datetime(2017, 8, 24, 0, 0)

def test_datetime_dateutil_strict_accept():
    assert perform_cast(
        "2017-08-24 something",
        sa.DateTime(),
        on_error=None,
    ) is None

def test_datetime_dateutil_exact_match():
    assert perform_cast(
        "2017-08-24",
        sa.DateTime(),
        on_error=None,
    ) == datetime(2017, 8, 24, 0, 0)


def test_datetime_rejects_invalid():
    assert perform_cast("2017-99-99", sa.DateTime(), on_error=None) is None

def test_datetime_rejects_garbage():
    assert perform_cast("not a date", sa.DateTime(), on_error=None) is None
