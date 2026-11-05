import pytest
import sqlalchemy as sa

from tests.support import read, unreadable


@pytest.mark.parametrize(
    "raw, expected",
    [("true", True), ("T", True), ("Yes", True), ("on", True), ("1", True),
     ("false", False), ("f", False), ("NO", False), ("off", False), ("0", False)],
)
def test_boolean_words(raw, expected):
    assert read(raw, sa.Boolean()) is expected


def test_boolean_rejects_unknown_words():
    assert unreadable("maybe", sa.Boolean())
