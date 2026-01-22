import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.orm import DeclarativeBase, Session
from pathlib import Path
import pandas as pd
import pytest

from orm_loader.tables.loadable_table import CSVLoadableTableInterface
from orm_loader.loaders.loader_interface import PandasLoader


class Base(DeclarativeBase):
    pass


class TestTable(Base, CSVLoadableTableInterface):
    __tablename__ = "test_table"

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


class RequiredTable(Base, CSVLoadableTableInterface):
    __tablename__ = "required_table"

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


class CompositeTable(Base, CSVLoadableTableInterface):
    __tablename__ = "composite_table"

    a: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    b: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    value: so.Mapped[str] = so.mapped_column(sa.String)


@pytest.fixture
def engine():
    engine = sa.create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def session(engine):
    with Session(engine) as session:
        yield session


@pytest.fixture
def tmp_csv_dir(tmp_path: Path) -> Path:
    return tmp_path


def test_initial_csv_load(session, tmp_csv_dir):
    csv_path = tmp_csv_dir / "test_table.csv"

    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
            {"id": 3, "name": "gamma"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    loader = PandasLoader()

    inserted = TestTable.load_csv( # type: ignore
        session,
        csv_path,
        dedupe=False,
        loader=loader,
    )
    session.commit()

    assert inserted == 3

    rows = session.execute(
        sa.select(TestTable).order_by(TestTable.id)
    ).scalars().all()

    assert [(r.id, r.name) for r in rows] == [
        (1, "alpha"),
        (2, "beta"),
        (3, "gamma"),
    ]


def test_replace_merge_strategy(session, tmp_csv_dir):
    csv_path = tmp_csv_dir / "test_table.csv"

    # Initial load
    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
            {"id": 3, "name": "gamma"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    loader = PandasLoader()

    TestTable.load_csv( # type: ignore
        session,
        csv_path,
        dedupe=False,
        loader=loader,
    )
    session.commit()

    # Replace rows 2 and 3
    pd.DataFrame(
        [
            {"id": 2, "name": "beta_updated"},
            {"id": 3, "name": "gamma_updated"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    replaced = TestTable.load_csv( # type: ignore
        session,
        csv_path,
        dedupe=False,
        loader=loader,
        merge_strategy="replace",
    )
    session.commit()

    assert replaced == 2

    rows = session.execute(
        sa.select(TestTable).order_by(TestTable.id)
    ).scalars().all()

    assert [(r.id, r.name) for r in rows] == [
        (1, "alpha"),
        (2, "beta_updated"),
        (3, "gamma_updated"),
    ]


def test_empty_csv_is_noop(session, tmp_csv_dir):
    csv_path = tmp_csv_dir / "test_table.csv"
    csv_path.touch()

    loader = PandasLoader()

    inserted = TestTable.load_csv( # type: ignore
        session, 
        csv_path,
        dedupe=False,
        loader=loader,
    )
    session.commit()

    assert inserted == 0

    rows = session.execute(sa.select(TestTable)).scalars().all()
    assert rows == []



def test_required_column_violation_drops_rows(session, tmp_path):
    Base.metadata.create_all(session.get_bind())

    csv = tmp_path / "required_table.csv"
    pd.DataFrame(
        [
            {"id": 1, "name": "ok"},
            {"id": 2, "name": None},  # invalid
        ]
    ).to_csv(csv, index=False)

    inserted = RequiredTable.load_csv( # type: ignore
        session,
        csv,
        loader=PandasLoader(),
        dedupe=False,
    )
    session.commit()

    assert inserted == 1



def test_composite_pk_dedup(session, tmp_path):
    Base.metadata.create_all(session.get_bind())

    csv = tmp_path / "composite_table.csv"
    pd.DataFrame(
        [
            {"a": 1, "b": 1, "value": "x"},
            {"a": 1, "b": 1, "value": "x"},
            {"a": 1, "b": 2, "value": "y"},
        ]
    ).to_csv(csv, index=False)

    inserted = CompositeTable.load_csv( # type: ignore
        session,
        csv,
        loader=PandasLoader(),
        dedupe=True,
    )
    session.commit()

    assert inserted == 2