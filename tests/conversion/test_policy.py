import pyarrow as pa
import pytest
import sqlalchemy as sa

from orm_loader.conversion import (
    BooleanHandler,
    ConversionPolicy,
    ConvertingHandler,
    DateHandler,
    DateTimeHandler,
    EnumHandler,
    ErrorPolicy,
    FloatHandler,
    IntegerHandler,
    Interpretation,
    NullPolicy,
    NumericHandler,
    PassthroughHandler,
    TextHandler,
)
from orm_loader.helpers import PolicyError
from tests.support import interpret, read, unreadable


class _Money(sa.Numeric):
    pass


@pytest.mark.parametrize(
    "sa_type, handler",
    [
        (sa.String(), TextHandler),
        (sa.Text(), TextHandler),
        (sa.Enum("a", "b"), EnumHandler),
        (sa.Integer(), IntegerHandler),
        (sa.BigInteger(), IntegerHandler),
        (sa.Float(), FloatHandler),
        (sa.Numeric(10, 2), NumericHandler),
        (_Money(10, 2), NumericHandler),
        (sa.Boolean(), BooleanHandler),
        (sa.Date(), DateHandler),
        (sa.DateTime(), DateTimeHandler),
        (sa.TIMESTAMP(), DateTimeHandler),
    ],
)
def test_the_most_specific_type_class_selects_the_handler(sa_type, handler):
    assert type(ConversionPolicy().for_column(sa_type)) is handler


class _Wrapped(sa.TypeDecorator):
    impl = sa.Integer
    cache_ok = True


def test_a_type_without_a_handler_is_an_error_naming_the_column():
    with pytest.raises(PolicyError, match="t.c: no handler for column type LargeBinary"):
        ConversionPolicy().for_column(sa.LargeBinary(), "t", "c")


def test_passthrough_is_an_explicit_choice_for_a_column():
    policy = ConversionPolicy(columns={("t", "c"): PassthroughHandler()})
    raw = pa.array([b"a", None])
    assert interpret(raw, sa.LargeBinary(), policy, table_name="t", column_name="c")[0] == [b"a", None]


def test_a_type_decorator_is_handled_as_the_type_it_stores():
    assert type(ConversionPolicy().for_column(_Wrapped())) is IntegerHandler


class _Hex(ConvertingHandler):
    def arrow_type(self, sa_type):
        return pa.int64()

    def parse(self, cleaned, sa_type):
        values = [int(v, 16) if v is not None else None for v in cleaned.to_pylist()]
        return Interpretation(pa.array(values, pa.int64()), pa.array([False] * len(values)))


def test_a_new_type_is_one_handler_and_one_registration():
    policy = ConversionPolicy(handlers={sa.LargeBinary: _Hex()})
    assert interpret(["ff", " 10 ", "", "NA"], sa.LargeBinary(), policy)[0] == [255, 16, None, None]


def test_policy_handlers_replace_the_built_in_one_for_their_class():
    policy = ConversionPolicy(handlers={sa.Integer: _Hex()})
    assert read("ff", sa.Integer(), policy) == 255
    assert read("1.5", sa.Float(), policy) == 1.5


def test_a_handler_carries_its_own_settings():
    policy = ConversionPolicy(boolean=BooleanHandler(true_words=("si",), false_words=("no",)))
    assert read("SI", sa.Boolean(), policy) is True
    assert read("no", sa.Boolean(), policy) is False
    assert unreadable("yes", sa.Boolean(), policy)


def test_date_and_datetime_formats_are_declared_separately():
    policy = ConversionPolicy(date=DateHandler(formats=("dd/mm/yyyy",)))
    assert read("02/01/2020", sa.Date(), policy).isoformat() == "2020-01-02"
    assert unreadable("02/01/2020", sa.DateTime(), policy)


def test_column_handlers_win_over_type_handlers():
    policy = ConversionPolicy(columns={("t", "c"): IntegerHandler()})
    assert read("7", sa.String(), policy, table_name="t", column_name="c") == 7
    assert read("7", sa.String(), policy, table_name="t", column_name="other") == "7"


def test_the_handler_and_column_mappings_cannot_be_changed_afterwards():
    policy = ConversionPolicy(columns={("t", "c"): IntegerHandler()})
    with pytest.raises(TypeError):
        policy.columns[("t", "d")] = IntegerHandler()  # type: ignore[index]


def test_unknown_error_mode_is_rejected():
    with pytest.raises(ValueError, match="on_error"):
        ErrorPolicy(on_error="ignore")  # type: ignore[arg-type]


def test_unknown_marker_quoting_is_rejected():
    with pytest.raises(ValueError, match="marker_quoting"):
        NullPolicy(marker_quoting="sometimes")  # type: ignore[arg-type]
