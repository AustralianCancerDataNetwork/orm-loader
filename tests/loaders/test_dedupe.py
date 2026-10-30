from typing import Type, cast

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import Role
from sqlalchemy.orm import DeclarativeBase

from orm_loader.tables.loadable_table import FileLoadableTableInterface
from orm_loader.tables.typing import FileTableProtocol


class Base(DeclarativeBase):
    pass


class DedupTable(Base, FileLoadableTableInterface):
    __tablename__ = "dedup_table"
    __table_args__ = {"schema": Role.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    value: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


_DedupTable = cast(Type[FileTableProtocol], DedupTable)


def test_internal_deduplication(sqlite_session, sqlite_engine, tmp_path):
    Base.metadata.create_all(sqlite_engine)

    csv = tmp_path / "dedup_table.csv"
    pd.DataFrame(
        [
            {"id": 1, "value": "a"},
            {"id": 1, "value": "a"},
            {"id": 2, "value": "b"},
        ]
    ).to_csv(csv, index=False)

    inserted = _DedupTable.load_file(
        sqlite_session,
        csv,
        dedupe=True,
    )
    sqlite_session.commit()

    assert inserted == 2