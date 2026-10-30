import time
from decimal import Decimal

import pyarrow as pa
import pytest
import sqlalchemy as sa

from tests.support import interpret, read, unreadable


@pytest.mark.parametrize("raw", ["12abc", "0x1F", "1.5", "1e3", "--1", "Infinity", "1.0.0"])
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


@pytest.mark.parametrize("raw", ["1.0", "1.00", "1.", "+2.0", "-3.000"])
def test_integer_accepts_integer_valued_decimals(raw):
    assert read(raw, sa.Integer()) == int(float(raw))


@pytest.mark.parametrize(
    "sa_type, low, high",
    [
        (sa.SmallInteger(), -(2**15), 2**15 - 1),
        (sa.Integer(), -(2**31), 2**31 - 1),
        (sa.BigInteger(), -(2**63), 2**63 - 1),
    ],
)
def test_integer_width_follows_the_column_type(sa_type, low, high):
    assert read(str(low), sa_type) == low
    assert read(str(high), sa_type) == high
    assert unreadable(str(low - 1), sa_type)
    assert unreadable(str(high + 1), sa_type)


def test_integer_far_beyond_64_bits_is_unreadable():
    assert unreadable("1" + "0" * 40, sa.BigInteger())


def test_float_rejects_infinity_from_text_overflow():
    assert unreadable("1e999", sa.Float())
    assert read("1e300", sa.Float()) == 1e300


def test_numeric_precision_without_scale_has_scale_zero():
    assert read("12345678901234567890", sa.Numeric(30)) == Decimal("12345678901234567890")
    assert unreadable("1.5", sa.Numeric(30))


def test_numeric_without_precision_is_exact_and_rejects_what_does_not_fit():
    assert read("12345678901234567890.123456789", sa.Numeric()) == Decimal("12345678901234567890.123456789000000000")
    assert unreadable("1" + "0" * 30, sa.Numeric())
    assert unreadable("0.1234567890123456789", sa.Numeric())


def test_numeric_digits_beyond_the_precision_do_not_wrap_around():
    assert unreadable("9" * 39, sa.Numeric(38, 0))
    assert unreadable("9" * 45, sa.Numeric(38, 0))
    assert read("9" * 38, sa.Numeric(38, 0)) == Decimal("9" * 38)


def _reference_integer(raw: str, bits: int) -> int | None:
    text = raw.strip()
    sign = -1 if text.startswith("-") else 1
    digits = text[1:] if text[:1] in ("+", "-") else text
    whole, _, fraction = digits.partition(".")
    if not (whole.isascii() and whole.isdigit()) or (fraction and set(fraction) != {"0"}) or text.count(".") > 1:
        return None
    value = sign * int(whole)
    return value if -(2 ** (bits - 1)) <= value < 2 ** (bits - 1) else None


EDGE_INTEGERS = [
    "0", "-0", "+0", "007", "-007", "1.0", "1.", ".5", "5.", "1.50", "+7", "--1", "1e3", "",
    "32767", "32768", "-32768", "-32769", "2147483647", "2147483648", "-2147483649",
    "9223372036854775807", "9223372036854775808", "-9223372036854775808", "-9223372036854775809",
    "0" * 30 + "12", "9" * 25, "12abc", " 7 ", "1_000", "١٢",
]


@pytest.mark.parametrize("sa_type, bits", [(sa.SmallInteger(), 16), (sa.Integer(), 32), (sa.BigInteger(), 64)])
def test_integer_parsing_matches_a_per_element_reference(sa_type, bits):
    values, invalid = interpret(EDGE_INTEGERS, sa_type)
    for raw, value, bad in zip(EDGE_INTEGERS, values, invalid):
        if raw.strip() == "":
            assert value is None and not bad, raw
            continue
        expected = _reference_integer(raw, bits)
        assert (value, bad) == ((expected, False) if expected is not None else (None, True)), raw


def test_one_bad_value_in_a_large_batch_does_not_slow_the_whole_batch():
    raw = pa.array([str(i) for i in range(200_000)])
    raw = pa.concat_arrays([raw.slice(0, 100_000), pa.array(["9" * 25]), raw.slice(100_000)])
    started = time.perf_counter()
    values, invalid = interpret(raw, sa.BigInteger())
    assert time.perf_counter() - started < 0.5
    assert sum(invalid) == 1 and invalid[100_000]


def test_numeric_fractions_longer_than_the_decimal_do_not_wrap_around():
    values = ["1.5" + "0" * 45, "1.50" + "0" * 40, "0." + "1" * 50, "1.234", "1.2300"]
    assert interpret(values, sa.Numeric(10, 2)) == (
        [Decimal("1.50"), Decimal("1.50"), None, None, Decimal("1.23")],
        [False, False, True, True, False],
    )


def test_numeric_with_scale_zero_accepts_a_zero_fraction_only():
    assert interpret(["2", "2.0", "2.5", "."], sa.Numeric(30)) == (
        [Decimal("2"), Decimal("2"), None, None],
        [False, False, True, True],
    )
