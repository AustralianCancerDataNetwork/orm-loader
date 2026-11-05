from datetime import date, datetime, timezone

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest
import sqlalchemy as sa

from orm_loader.conversion import (
    ConversionPolicy,
    DateFormats,
    DateHandler,
    parse_dates,
)
from tests.support import interpret, read, unreadable


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("20200229", date(2020, 2, 29)),
        ("24-AUG-2017", date(2017, 8, 24)),
        ("24-aug-2017", date(2017, 8, 24)),
        ("2017-08-24", date(2017, 8, 24)),
        (" 20200101 ", date(2020, 1, 1)),
    ],
)
def test_both_athena_formats_and_iso_parse_by_default(raw, expected):
    assert read(raw, sa.Date()) == expected


@pytest.mark.parametrize(
    "raw",
    ["20200231", "19000229", "2020022", "+2020229", "202002299", "abcdefgh",
     "31-FEB-2020", "01-XXX-2020", "2019-02-29", "Jan 2 2020", "2020-13-45"],
)
def test_impossible_or_misshapen_dates_are_unreadable(raw):
    assert unreadable(raw, sa.Date())


def test_ambiguous_date_needs_a_declared_format():
    assert unreadable("02/01/2020", sa.Date())
    assert read("02/01/2020", sa.Date(), ConversionPolicy(date=DateHandler(formats=("dd/mm/yyyy",)))) == date(2020, 1, 2)
    assert read("02/01/2020", sa.Date(), ConversionPolicy(date=DateHandler(formats=("mm/dd/yyyy",)))) == date(2020, 2, 1)


def test_unknown_date_format_name_is_rejected():
    with pytest.raises(ValueError, match="Unknown date formats"):
        DateHandler(formats=("yyyy/dd/mm",))


def test_dictionary_encoded_dates_parse_the_same():
    column = pc.dictionary_encode(pa.array(["20200229", None, "", "20200301", "20200229"]))
    assert interpret(column, sa.Date())[0] == [
        date(2020, 2, 29), None, None, date(2020, 3, 1), date(2020, 2, 29),
    ]


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("2017-08-24T12:34:56", datetime(2017, 8, 24, 12, 34, 56)),
        ("2017-08-24 12:34", datetime(2017, 8, 24, 12, 34)),
        ("2017-08-24 12:34:56.123456", datetime(2017, 8, 24, 12, 34, 56, 123456)),
        ("20170824", datetime(2017, 8, 24)),
        ("24-AUG-2017", datetime(2017, 8, 24)),
    ],
)
def test_datetime_accepts_iso_and_date_only_forms(raw, expected):
    assert read(raw, sa.DateTime()) == expected


@pytest.mark.parametrize("raw", ["2017-02-30 01:00:00", "2017-08-24 25:00:00", "2017-08-24 something", "garbage"])
def test_datetime_rejects_invalid_values(raw):
    assert unreadable(raw, sa.DateTime())


def _yyyymmdd(days: np.ndarray) -> pa.Array:
    years = days.astype("datetime64[Y]").astype(int) + 1970
    months = days.astype("datetime64[M]").astype(int) % 12 + 1
    dom = (days - days.astype("datetime64[M]")).astype(int) + 1

    def pad(values: np.ndarray, width: int) -> pa.Array:
        return pc.utf8_lpad(pa.array(values).cast(pa.string()), width, "0")

    return pc.binary_join_element_wise(pad(years, 4), pad(months, 2), pad(dom, 2), "")


def test_every_calendar_date_from_year_1_to_9999_parses_to_itself():
    days = np.arange(np.datetime64("0001-01-01"), np.datetime64("10000-01-01"), dtype="datetime64[D]")
    dates, invalid = parse_dates(_yyyymmdd(days), ("yyyymmdd",))
    assert not pc.any(invalid).as_py()
    assert dates.cast(pa.int32()).to_numpy().tolist() == days.astype(int).tolist()


@pytest.mark.parametrize("year", [1900, 2000, 2019, 2020, 2100])
def test_month_day_grid_accepts_exactly_what_the_calendar_accepts(year):
    grid = [(m, d) for m in range(100) for d in range(100)]
    values = pa.array([f"{year:04d}{m:02d}{d:02d}" for m, d in grid])
    _, invalid = parse_dates(values, ("yyyymmdd",))
    expected = []
    for m, d in grid:
        try:
            date(year, m, d)
            expected.append(False)
        except ValueError:
            expected.append(True)
    assert invalid.to_pylist() == expected


def test_arrow_strptime_still_rolls_impossible_dates_over():
    rolled = pc.strptime(pa.array(["20200231"]), format="%Y%m%d", unit="s")
    assert rolled.cast(pa.date32()).to_pylist() != [None], (
        "Arrow strptime now rejects impossible dates (apache/arrow#54643): "
        "replace the ISO reassembly in parse_dates with strptime"
    )


def test_default_formats_are_mutually_distinguishable():
    probes = ["20200229", "29-FEB-2020", "2020-02-29"]
    for name in DateFormats.DEFAULT:
        matches = [bool(DateFormats.SHAPES[name](pa.array([p]))[0][0].as_py()) for p in probes]
        assert sum(matches) == 1


def test_typed_input_is_cast_without_parsing():
    assert read(date(2020, 5, 17), sa.Date()) == date(2020, 5, 17)
    assert read(datetime(2020, 5, 17), sa.Date()) == date(2020, 5, 17)
    assert read(datetime(2020, 5, 17, 14, 30, 5), sa.DateTime()) == datetime(2020, 5, 17, 14, 30, 5)
    assert read(date(2020, 5, 17), sa.DateTime()) == datetime(2020, 5, 17)


def test_typed_timestamp_with_a_time_is_not_a_date():
    assert unreadable(datetime(2020, 5, 17, 14, 30), sa.Date())


@pytest.mark.parametrize("sa_type", [sa.Date(), sa.DateTime()])
def test_year_zero_is_unreadable(sa_type):
    assert unreadable("0000-01-01", sa_type)
    assert read("0001-01-01", sa_type) is not None


def test_a_timezone_column_requires_an_offset_and_stores_utc():
    aware = sa.DateTime(timezone=True)
    assert read("2020-01-01T10:00:00+10:00", aware) == datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert read("2020-01-01 10:00:00Z", aware) == datetime(2020, 1, 1, 10, tzinfo=timezone.utc)
    assert unreadable("2020-01-01 10:00:00", aware)
    assert unreadable("20200101", aware)


def test_a_column_without_a_time_zone_rejects_an_offset():
    assert read("2020-01-01 10:00:00", sa.DateTime()) == datetime(2020, 1, 1, 10)
    assert unreadable("2020-01-01T10:00:00+10:00", sa.DateTime())
    assert unreadable("2020-01-01T10:00:00Z", sa.DateTime())


def test_a_batch_with_one_bad_date_isolates_it():
    values, invalid = interpret(["20200101"] * 1000 + ["20200231"] + ["20200102"] * 1000, sa.Date())
    assert [i for i, bad in enumerate(invalid) if bad] == [1000]
    assert values[0] == date(2020, 1, 1) and values[-1] == date(2020, 1, 2)
