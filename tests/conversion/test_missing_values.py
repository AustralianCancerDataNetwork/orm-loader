from datetime import date

import pyarrow as pa
import pytest
import sqlalchemy as sa

from tests.support import interpret, read


def test_zero_false_and_dates_are_values():
    assert read(0, sa.Integer()) == 0
    assert read(False, sa.Boolean()) is False
    assert read(date(2020, 2, 29), sa.Date()) == date(2020, 2, 29)


def test_arrow_nulls_and_nan_arrays():
    assert interpret(pa.nulls(3, pa.int64()), sa.Integer())[0] == [None] * 3
    assert interpret(pa.nulls(2, pa.string()), sa.String())[0] == [None, None]
    for dtype in (pa.float32(), pa.float64()):
        values, invalid = interpret(pa.array([1.0, float("nan"), 0.0], dtype), sa.Float())
        assert values == [1.0, None, 0.0]
        assert not any(invalid)


@pytest.mark.parametrize("word", ["null", "None", "NA", "N/A", "NaN", "#N/A", "-nan", "<NA>"])
def test_typed_columns_read_missing_words_as_null(word):
    for sa_type in (sa.Integer(), sa.Float(), sa.Date(), sa.Boolean(), sa.DateTime()):
        assert read(word, sa_type) is None
