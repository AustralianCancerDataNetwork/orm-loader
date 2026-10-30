import pyarrow.compute as pc
import pytest
import sqlalchemy as sa

from orm_loader.conversion import ConversionPolicy, NullPolicy, TextHandler
from tests.support import interpret, read, unreadable

MARKER = ConversionPolicy(nulls=NullPolicy(text_markers=frozenset({"NULL"})))


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("NULL", None),
        (" NULL ", None),
        ('"NULL"', '"NULL"'),
        ("Null", "Null"),
        ("null", "null"),
        ("NULLIFY", "NULLIFY"),
        ("Contains NULL here", "Contains NULL here"),
        ("NA", "NA"),
        ("", None),
        ("  ", None),
    ],
)
def test_text_marker_matches_only_the_exact_word(raw, expected):
    assert read(raw, sa.String(), MARKER) == expected


def test_default_policy_has_no_text_markers():
    assert read("NULL", sa.String()) == "NULL"


class _DashIsMissing(TextHandler):
    def missing(self, trimmed, nulls):
        return pc.equal(trimmed, "-")


def test_a_text_handler_subclass_can_choose_its_own_missing_values():
    policy = ConversionPolicy(
        nulls=NullPolicy(text_markers=frozenset({"NULL"})),
        columns={("t", "a"): _DashIsMissing()},
    )
    assert read("-", sa.String(), policy, table_name="t", column_name="a") is None
    assert read("NULL", sa.String(), policy, table_name="t", column_name="a") == "NULL"
    assert read("NULL", sa.String(), policy, table_name="t", column_name="b") is None
    assert read("-", sa.String(), policy, table_name="t", column_name="b") == "-"


def test_policies_do_not_leak_between_conversions():
    assert read("NULL", sa.String(), MARKER) is None
    assert read("NULL", sa.String()) == "NULL"
    assert read("NULL", sa.String(), MARKER) is None


def test_padding_is_stripped_unless_disabled():
    assert read("  x  ", sa.String()) == "x"
    assert read("  x  ", sa.String(), ConversionPolicy(text=TextHandler(strip=False))) == "  x  "


def test_blank_text_is_kept_as_empty_when_empty_is_not_null():
    assert read("  ", sa.String(), ConversionPolicy(nulls=NullPolicy(empty_is_null=False))) == ""


def test_text_longer_than_the_column_raises():
    assert unreadable("ab", sa.String(1))


def test_an_enum_column_accepts_only_its_labels():
    enum = sa.Enum("red", "green", name="colour")
    values, invalid = interpret(["red", " green ", "blue", ""], enum)
    assert values == ["red", "green", None, None]
    assert invalid == [False, False, True, False]
