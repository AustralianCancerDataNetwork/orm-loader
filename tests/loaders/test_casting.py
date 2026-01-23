import sqlalchemy as sa
from orm_loader.loaders.data.converters import perform_cast

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