import pyarrow as pa
from orm_loader.loaders.loading_helpers import arrow_drop_duplicates
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.orm import DeclarativeBase
from orm_loader.tables.loadable_table import CSVLoadableTableInterface
from orm_loader.loaders.loader_interface import PandasLoader


class Base(DeclarativeBase):
    pass


class DedupTable(Base, CSVLoadableTableInterface):
    __tablename__ = "dedup_table"

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    value: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


def test_arrow_drop_duplicates_simple():
    table = pa.table({
        "id": [1, 1, 2],
        "x":  ["a", "a", "b"],
    })

    deduped = arrow_drop_duplicates(table, ["id"])
    assert deduped.num_rows == 2
    assert deduped["id"].to_pylist() == [1, 2]



def test_internal_deduplication(session, tmp_path):
    Base.metadata.create_all(session.get_bind())

    csv = tmp_path / "dedup_table.csv"
    pd.DataFrame(
        [
            {"id": 1, "value": "a"},
            {"id": 1, "value": "a"},
            {"id": 2, "value": "b"},
        ]
    ).to_csv(csv, index=False)

    inserted = DedupTable.load_csv( # type: ignore
        session,
        csv,
        loader=PandasLoader(),
        dedupe=True,
    )
    session.commit()

    assert inserted == 2