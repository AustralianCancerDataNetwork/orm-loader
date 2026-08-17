from enum import Enum, IntEnum

import sqlalchemy as sa
from orm_loader.loaders.data.converters import (
    _COLUMN_CAST_RULES,
    cast_scalar,
    perform_cast,
    register_column_cast_rule,
)
import pytest
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


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("NULL", None),
        (" null ", None),
        ("NaN", None),
        ("N/A", None),
        ("none", None),
        ("", None),
        (None, None),
    ],
)
def test_string_null_normalisation(raw, expected):
    assert cast_scalar(raw, sa.String()) is expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("NULL", None),
        ("  NULL  ", None),
        ("NaN", None),
        ("", None),
        ("123", 123),
    ],
)
def test_numeric_null_normalisation(raw, expected):
    assert cast_scalar(raw, sa.Integer()) == expected


def test_cast_float_accepts_decimal_strings():
    assert cast_scalar("1.80", sa.Float()) == 1.8
    assert cast_scalar("2.50", sa.Float()) == 2.5
    assert cast_scalar("2", sa.Float()) == 2.0


def test_cast_int_rejects_decimal_strings():
    assert cast_scalar("1.80", sa.Integer()) is None
    assert cast_scalar("2.5", sa.Integer()) is None
    assert cast_scalar("2", sa.Integer()) == 2


class Role(str, Enum):
    FIRST_AUTHOR = "first author"
    LAST_AUTHOR = "last author"


@pytest.fixture(autouse=True)
def _clear_column_cast_rules():
    # _COLUMN_CAST_RULES is process-global state; isolate each test so
    # registrations here can't leak into other test modules (or later tests
    # in this file) that also call register_column_cast_rule.
    _COLUMN_CAST_RULES.clear()
    yield
    _COLUMN_CAST_RULES.clear()


def test_register_column_cast_rule_requires_scalar_or_enum_type():
    with pytest.raises(ValueError, match="exactly one"):
        register_column_cast_rule("t", "c")


def test_register_column_cast_rule_rejects_both_scalar_and_enum_type():
    with pytest.raises(ValueError, match="exactly one"):
        register_column_cast_rule("t", "c", scalar=lambda v: v, enum_type=Role)


def test_column_cast_rule_enum_type_resolves_known_value():
    # Matches by .value (the raw display text) but returns .name -- sa.Enum's
    # own default column-storage convention.
    register_column_cast_rule("authors", "role", enum_type=Role)
    assert cast_scalar("first author", sa.String(), table_name="authors", column_name="role") == "FIRST_AUTHOR"


def test_column_cast_rule_enum_type_records_unknown_value():
    errors = []
    register_column_cast_rule("authors", "role", enum_type=Role)
    result = cast_scalar(
        "BOGUS", sa.String(), on_error=errors.append, table_name="authors", column_name="role"
    )
    assert result is None
    assert errors == ["BOGUS"]


def test_column_cast_rule_takes_precedence_over_type_based_rules():
    # sa.Enum is itself a subclass of sa.String, so without column-specific
    # precedence the existing String CastRule would match first and this
    # would just pass "first author" through untouched, never resolving it
    # to the Role member.
    register_column_cast_rule("authors", "role", enum_type=Role)
    result = cast_scalar("first author", sa.Enum(Role), table_name="authors", column_name="role")
    assert result == "FIRST_AUTHOR"


def test_column_with_no_registered_rule_is_unaffected():
    register_column_cast_rule("authors", "role", enum_type=Role)
    # A different column on the same table falls through to ordinary type-based casting
    assert cast_scalar("42", sa.Integer(), table_name="authors", column_name="other") == 42


def test_perform_cast_threads_table_and_column_name():
    register_column_cast_rule("authors", "role", enum_type=Role)
    assert (
        perform_cast("last author", sa.String(), on_error=None, table_name="authors", column_name="role")
        == "LAST_AUTHOR"
    )


def test_register_column_cast_rule_replaces_not_stacks():
    register_column_cast_rule("t", "c", scalar=lambda v: "first")
    register_column_cast_rule("t", "c", scalar=lambda v: "second")
    assert cast_scalar("anything", sa.String(), table_name="t", column_name="c") == "second"


def test_custom_scalar_cast_rule():
    register_column_cast_rule("t", "c", scalar=lambda v: v.upper())
    assert cast_scalar("shout", sa.String(), table_name="t", column_name="c") == "SHOUT"

class Grade(IntEnum):
    LOW = 1
    HIGH = 2


def test_enum_type_matches_non_string_member_values():
    # sa.Enum(IntEnum) is legal and still persists .name, so an int-valued
    # source column has to resolve without a lossy str() round-trip.
    register_column_cast_rule("g", "grade", enum_type=Grade)
    assert cast_scalar(1, sa.Enum(Grade), table_name="g", column_name="grade") == "LOW"
    assert cast_scalar(2.0, sa.Enum(Grade), table_name="g", column_name="grade") == "HIGH"


def test_enum_type_still_matches_stringified_non_string_values():
    # A CSV source reads as str even for an int-valued enum, so the str()
    # fallback has to stay behind the raw-value attempt.
    register_column_cast_rule("g", "grade", enum_type=Grade)
    assert cast_scalar("1", sa.Enum(Grade), table_name="g", column_name="grade") == "LOW"


def test_enum_type_accepts_an_already_resolved_member():
    register_column_cast_rule("authors", "role", enum_type=Role)
    assert (
        cast_scalar(Role.FIRST_AUTHOR, sa.Enum(Role), table_name="authors", column_name="role")
        == "FIRST_AUTHOR"
    )
    register_column_cast_rule("g", "grade", enum_type=Grade)
    assert cast_scalar(Grade.HIGH, sa.Enum(Grade), table_name="g", column_name="grade") == "HIGH"


def test_enum_type_unknown_value_still_fails():
    # The widened matching must not start accepting non-members.
    register_column_cast_rule("g", "grade", enum_type=Grade)
    errors = []
    assert cast_scalar(
        99, sa.Enum(Grade), table_name="g", column_name="grade", on_error=errors.append
    ) is None
    assert errors == [99]


@pytest.mark.parametrize("raw", ["   ", "", None])
def test_enum_type_blank_values_are_null_not_failures(raw):
    register_column_cast_rule("authors", "role", enum_type=Role)
    errors = []
    assert cast_scalar(
        raw, sa.Enum(Role), table_name="authors", column_name="role", on_error=errors.append
    ) is None
    assert errors == []
