import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.orm import DeclarativeBase
from typing import cast, Type

from orm_loader.tables.loadable_table import CSVLoadableTableInterface
from orm_loader.tables.typing import CSVTableProtocol
from orm_loader.loaders.loader_interface import ParquetLoader


class Base(DeclarativeBase):
    pass


class ParquetTable(Base, CSVLoadableTableInterface):
    __tablename__ = "parquet_table"

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    value: so.Mapped[int] = so.mapped_column(sa.Integer, nullable=False)


_ParquetTable = cast(Type[CSVTableProtocol], ParquetTable)


def test_parquet_loader(session, engine, tmp_path):
    Base.metadata.create_all(engine)

    df = pd.DataFrame(
        [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
        ]
    )
    table = pa.Table.from_pandas(df)
    path = tmp_path / "parquet_table.parquet"
    pq.write_table(table, path)

    inserted = _ParquetTable.load_csv(
        session,
        path,
        loader=ParquetLoader(),
        dedupe=False,
    )
    session.commit()

    assert inserted == 2
