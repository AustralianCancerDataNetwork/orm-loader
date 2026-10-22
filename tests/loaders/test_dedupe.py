from typing import cast

import pandas as pd
import pyarrow as pa
import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import Role
from sqlalchemy.orm import DeclarativeBase

from orm_loader.loaders.loader_interface import PandasLoader
from orm_loader.loaders.loading_helpers import arrow_drop_duplicates
from orm_loader.tables.loadable_table import CSVLoadableTableInterface
from orm_loader.tables.typing import CSVTableProtocol


class Base(DeclarativeBase):
    pass


class DedupTable(Base, CSVLoadableTableInterface):
    __tablename__ = "dedup_table"
    __table_args__ = {"schema": Role.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    value: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


_DedupTable = cast(type[CSVTableProtocol], DedupTable)


def test_arrow_drop_duplicates_simple():
    table = pa.table(
        {
            "id": [1, 1, 2],
            "x": ["a", "a", "b"],
        }
    )

    deduped = arrow_drop_duplicates(table, ["id"])
    assert deduped.num_rows == 2
    assert deduped["id"].to_pylist() == [1, 2]


def test_internal_deduplication(session, engine, tmp_path):
    Base.metadata.create_all(engine)

    csv = tmp_path / "dedup_table.csv"
    pd.DataFrame(
        [
            {"id": 1, "value": "a"},
            {"id": 1, "value": "a"},
            {"id": 2, "value": "b"},
        ]
    ).to_csv(csv, index=False)

    inserted = _DedupTable.load_csv(
        session,
        csv,
        loader=PandasLoader(),
        dedupe=True,
    )
    session.commit()

    assert inserted == 2
