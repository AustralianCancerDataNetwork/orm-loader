import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.orm import Session
from pathlib import Path
import pandas as pd
import pytest

from orm_loader.tables.loadable_table import CSVLoadableTableInterface
from orm_loader.loaders.loader_interface import PandasLoader
from orm_loader.helpers import Base

class SimpleTable(Base, CSVLoadableTableInterface):
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

    inserted = SimpleTable.load_csv( # type: ignore
        session,
        csv_path,
        dedupe=False,
        loader=loader,
    )
    session.commit()

    assert inserted == 3

    rows = session.execute(
        sa.select(SimpleTable).order_by(SimpleTable.id)
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

    SimpleTable.load_csv( # type: ignore
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

    replaced = SimpleTable.load_csv( # type: ignore
        session,
        csv_path,
        dedupe=False,
        loader=loader,
        merge_strategy="replace",
    )
    session.commit()

    assert replaced == 2

    rows = session.execute(
        sa.select(SimpleTable).order_by(SimpleTable.id)
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

    inserted = SimpleTable.load_csv( # type: ignore
        session, 
        csv_path,
        dedupe=False,
        loader=loader,
    )
    session.commit()

    assert inserted == 0

    rows = session.execute(sa.select(SimpleTable)).scalars().all()
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


@pytest.mark.parametrize(
    "merge_strategy,expected_rows,expected_inserted",
    [
        (
            "replace",
            [
                (1, "alpha"),
                (2, "beta_updated"),
                (3, "gamma_updated"),
            ],
            2,
        ),
        (
            "upsert",
            [
                (1, "alpha"),
                (2, "beta"),
                (3, "gamma"),
            ],
            2,
        ),
    ],
)
def test_merge_strategies(session, tmp_csv_dir, merge_strategy, expected_rows, expected_inserted):
    csv_path = tmp_csv_dir / "test_table.csv"

    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
            {"id": 3, "name": "gamma"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    loader = PandasLoader()
    SimpleTable.load_csv(session, csv_path, dedupe=False, loader=loader)
    session.commit()

    pd.DataFrame(
        [
            {"id": 2, "name": "beta_updated"},
            {"id": 3, "name": "gamma_updated"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    inserted = SimpleTable.load_csv(
        session,
        csv_path,
        dedupe=False,
        loader=loader,
        merge_strategy=merge_strategy,
    )
    session.commit()

    assert inserted == expected_inserted

    rows = session.execute(
        sa.select(SimpleTable).order_by(SimpleTable.id, SimpleTable.name)
    ).scalars().all()

    assert [(r.id, r.name) for r in rows] == expected_rows


def test_staging_table_is_created_and_dropped(session, tmp_csv_dir):
    csv_path = tmp_csv_dir / "test_table.csv"

    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv_path, index=False)

    SimpleTable.load_csv(
        session,
        csv_path,
        loader=PandasLoader(),
        dedupe=False,
    )
    session.commit()

    inspector = sa.inspect(session.get_bind())
    assert not inspector.has_table(SimpleTable.staging_tablename())


def test_composite_pk_replace_merge(session, tmp_path):
    csv = tmp_path / "composite_table.csv"

    pd.DataFrame(
        [
            {"a": 1, "b": 1, "value": "x"},
            {"a": 1, "b": 2, "value": "y"},
        ]
    ).to_csv(csv, index=False)

    CompositeTable.load_csv(session, csv, loader=PandasLoader())
    session.commit()

    pd.DataFrame(
        [
            {"a": 1, "b": 1, "value": "x_updated"},
        ]
    ).to_csv(csv, index=False)

    CompositeTable.load_csv(
        session,
        csv,
        loader=PandasLoader(),
        merge_strategy="replace",
    )
    session.commit()

    rows = session.execute(
        sa.select(CompositeTable).order_by(CompositeTable.a, CompositeTable.b)
    ).scalars().all()

    assert [(r.a, r.b, r.value) for r in rows] == [
        (1, 1, "x_updated"),
        (1, 2, "y"),
    ]


def test_filename_must_match_tablename(session, tmp_path):
    csv = tmp_path / "wrong_name.csv"
    pd.DataFrame([{"id": 1, "name": "x"}]).to_csv(csv, index=False)

    with pytest.raises(ValueError, match="does not match table"):
        SimpleTable.load_csv(
            session,
            csv,
            loader=PandasLoader(),
        )


@pytest.mark.postgres
def test_postgres_copy_fast_path(pg_session, tmp_path):
    csv = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv, index=False)

    inserted = SimpleTable.load_csv(pg_session, csv)
    pg_session.commit()

    assert inserted == 1
