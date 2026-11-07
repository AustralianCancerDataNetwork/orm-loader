"""Numeric columns load exactly the values their file holds, in text and in typed Parquet columns."""

from decimal import Decimal
from pathlib import Path
from typing import cast

import numpy as np
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so

from orm_loader.backends import STAGING_SCHEMA
from orm_loader.conversion import ConversionPolicy, ErrorPolicy
from orm_loader.conversion.handlers import PassthroughHandler
from orm_loader.helpers import ConversionError, PolicyError
from orm_loader.loaders import LoadReport
from orm_loader.tables import FileLoadableTableInterface
from orm_loader.tables.typing import FileTableProtocol


class AmountBase(so.DeclarativeBase):
    pass


class Amount(AmountBase, FileLoadableTableInterface):
    __tablename__ = "numeric_amount"
    record_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True, autoincrement=False)
    amount: so.Mapped[Decimal | None] = so.mapped_column(sa.Numeric(12, 2))


class WideAmount(AmountBase, FileLoadableTableInterface):
    __tablename__ = "numeric_wide_amount"
    record_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True, autoincrement=False)
    amount: so.Mapped[Decimal | None] = so.mapped_column(sa.Numeric(50, 2))


class UnboundedAmount(AmountBase, FileLoadableTableInterface):
    __tablename__ = "numeric_unbounded_amount"
    record_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True, autoincrement=False)
    amount: so.Mapped[Decimal | None] = so.mapped_column(sa.Numeric())


REJECT = ConversionPolicy(errors=ErrorPolicy(on_error="reject_row"))


def _load(session, path: Path, model, **kwargs) -> int:
    model.__table__.create(session.connection(), checkfirst=True)
    session.commit()
    tag = None if session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA
    loaded = cast(type[FileTableProtocol], model).load_file(
        session, path, staging_schema_tag=tag, **kwargs
    )
    session.commit()
    return loaded


def _amounts(session, model=Amount) -> dict[int, Decimal | None]:
    rows = session.execute(sa.select(model.record_id, model.amount).order_by(model.record_id)).all()
    return {key: None if value is None else Decimal(str(value)) for key, value in rows}


def _rejected(path: Path | None) -> list[tuple[str, str]]:
    assert path is not None
    header = [h.strip('"') for h in path.read_text().splitlines()[0].split(",")]
    table = pv.read_csv(
        path, convert_options=pv.ConvertOptions(column_types={name: pa.string() for name in header})
    )
    return [(row["row"], row["amount"]) for row in table.to_pylist()]


def test_text_amounts_load_exactly_and_the_rest_is_rejected_with_its_row(dialect_session, tmp_path):
    cells = [
        "1.50",
        " 2.5 ",
        ".05",
        "1.5E3",
        "1e-2",
        "007.50",
        "+7",
        "3.100",
        "5.",
        ".0",
        "1.5E+003",
        ".",
        "abc",
        "1.555",
        "1e10",
        "5e-50",
        "0e+-0",
        "1" + "0" * 40,
    ]
    path = tmp_path / "numeric_amount.csv"
    path.write_text(
        "record_id,amount\n" + "".join(f"{i},{cell}\n" for i, cell in enumerate(cells, 1))
    )
    report = LoadReport()
    assert _load(dialect_session, path, Amount, policy=REJECT, report=report) == 11
    assert _amounts(dialect_session) == {
        1: Decimal("1.50"),
        2: Decimal("2.50"),
        3: Decimal("0.05"),
        4: Decimal("1500.00"),
        5: Decimal("0.01"),
        6: Decimal("7.50"),
        7: Decimal("7.00"),
        8: Decimal("3.10"),
        9: Decimal("5.00"),
        10: Decimal("0.00"),
        11: Decimal("1500.00"),
    }
    assert (report.rows_read, report.rows_loaded, report.rows_rejected) == (18, 11, 7)
    assert report.accounted
    assert _rejected(report.rejects_path) == [
        ("12", "."),
        ("13", "abc"),
        ("14", "1.555"),
        ("15", "1e10"),
        ("16", "5e-50"),
        ("17", "0e+-0"),
        ("18", "1" + "0" * 40),
    ]


def test_a_lone_point_raises_a_conversion_error_before_it_reaches_the_database(
    dialect_session, tmp_path
):
    path = tmp_path / "numeric_amount.csv"
    path.write_text("record_id,amount\n1,.\n2,2.50\n")
    with pytest.raises(ConversionError, match="amount"):
        _load(dialect_session, path, Amount)
    assert _amounts(dialect_session) == {}


@pytest.mark.parametrize(
    "column, loaded, rejected",
    [
        (pa.array([5, 10**11, 7], pa.int64()), {1: "5.00", 3: "7.00"}, ["2"]),
        (pa.array([5, 10**11], pa.uint64()), {1: "5.00"}, ["2"]),
        (
            pa.array([Decimal("1.250"), Decimal("1.555"), Decimal("2.5")], pa.decimal256(55, 3)),
            {1: "1.25", 3: "2.50"},
            ["2"],
        ),
        (
            pa.array([Decimal("1.25"), Decimal("1.5")], pa.decimal128(10, 2)),
            {1: "1.25", 2: "1.50"},
            [],
        ),
        (
            pa.array([708693.7, 0.1 + 0.2, 2.5, float("inf"), float("nan")]),
            {1: "708693.70", 3: "2.50", 5: None},
            ["2", "4"],
        ),
        (pa.array(np.array([-549424.4, 0.1], np.float32)), {1: "-549424.40", 2: "0.10"}, []),
    ],
    ids=["int64", "uint64", "decimal256", "decimal128", "float64", "float32"],
)
def test_parquet_columns_load_exactly_or_are_rejected(
    dialect_session, tmp_path, column, loaded, rejected
):
    path = tmp_path / "numeric_amount.parquet"
    pq.write_table(
        pa.table({"record_id": pa.array(range(1, len(column) + 1), pa.int32()), "amount": column}),
        path,
    )
    report = LoadReport()
    _load(dialect_session, path, Amount, policy=REJECT, report=report)
    assert _amounts(dialect_session) == {
        key: None if value is None else Decimal(value) for key, value in loaded.items()
    }
    assert report.rows_rejected == len(rejected)
    if rejected:
        assert [row for row, _ in _rejected(report.rejects_path)] == rejected
    assert report.accounted


@pytest.mark.parametrize(
    "model, declared", [(WideAmount, r"\(50, 2\)"), (UnboundedAmount, r"\(None, None\)")]
)
def test_a_numeric_column_an_arrow_decimal_cannot_hold_is_refused_before_any_row_is_read(
    sqlite_session, tmp_path, model, declared
):
    path = tmp_path / f"{model.__tablename__}.csv"
    path.write_text("record_id,amount\n1,2.50\n")
    with pytest.raises(
        PolicyError, match=rf"{model.__tablename__}.amount.*{declared}.*PassthroughHandler"
    ):
        _load(sqlite_session, path, model)
    assert sqlite_session.scalar(sa.select(sa.func.count()).select_from(model)) == 0


@pytest.mark.parametrize(
    "model, expected",
    [
        (WideAmount, ["1.50", "0.05", "1500.00", "1.56", "1" + "0" * 40 + ".25"]),
        (UnboundedAmount, ["1.50", "0.05", "1500", "1.555", "1" + "0" * 40 + ".25"]),
    ],
)
def test_a_column_the_loader_cannot_convert_loads_when_left_to_the_database(
    pg_session, tmp_path, model, expected
):
    cells = ["1.50", ".05", "1.5E3", "1.555", "1" + "0" * 40 + ".25"]
    path = tmp_path / f"{model.__tablename__}.csv"
    path.write_text(
        "record_id,amount\n" + "".join(f"{i},{cell}\n" for i, cell in enumerate(cells, 1))
    )
    policy = ConversionPolicy(columns={(model.__tablename__, "amount"): PassthroughHandler()})
    assert _load(pg_session, path, model, policy=policy) == len(cells)
    assert list(_amounts(pg_session, model).values()) == [Decimal(value) for value in expected]
