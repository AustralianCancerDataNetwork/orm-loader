import pytest
import sqlalchemy as sa

from orm_loader.conversion import ConversionPolicy, NullPolicy, TextHandler
from tests.support import read, unreadable

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


def test_column_markers_replace_the_import_markers_for_their_column():
    policy = ConversionPolicy(
        nulls=NullPolicy(text_markers=frozenset({"NULL"})),
        columns={("t", "a"): TextHandler(markers=frozenset({"N/A"}))},
    )
    assert read("NULL", sa.String(), policy, table_name="t", column_name="a") == "NULL"
    assert read("N/A", sa.String(), policy, table_name="t", column_name="a") is None
    assert read("NULL", sa.String(), policy, table_name="t", column_name="b") is None
    assert read("N/A", sa.String(), policy, table_name="t", column_name="b") == "N/A"


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
