import sqlalchemy as sa
from orm_loader.loaders.data import perform_cast

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
