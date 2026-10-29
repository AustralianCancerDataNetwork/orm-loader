from decimal import Decimal

import pytest
import sqlalchemy as sa

from tests.support import read, unreadable


@pytest.mark.parametrize("raw", ["12abc", "0x1F", "1.0", "1e3", "--1", "Infinity"])
def test_integer_rejects_non_integer_text(raw):
    assert unreadable(raw, sa.Integer())


def test_integer_accepts_sign_and_leading_zeros():
    assert read("+5", sa.Integer()) == 5
    assert read("-007", sa.Integer()) == -7


@pytest.mark.parametrize("raw", ["Infinity", "inf", "-inf", "1,5", "abc"])
def test_float_rejects_non_numbers(raw):
    assert unreadable(raw, sa.Float())


def test_float_accepts_decimal_and_exponent_forms():
    assert [read(v, sa.Float()) for v in ("1.80", "2", ".5", "1e3", "+2.5")] == [1.8, 2.0, 0.5, 1000.0, 2.5]


def test_numeric_keeps_declared_precision_and_rejects_overflow_of_scale():
    assert read("1.50", sa.Numeric(10, 2)) == Decimal("1.50")
    assert unreadable("1.555", sa.Numeric(10, 2))


def test_integer_trims_padding_and_reads_blank_and_missing_words_as_null():
    assert read("  7  ", sa.Integer()) == 7
    assert [read(v, sa.Integer()) for v in ("", "  NULL  ", "NaN")] == [None, None, None]


def test_typed_input_is_cast_without_parsing():
    assert read(42.0, sa.Integer()) == 42
    assert unreadable(42.5, sa.Integer())
