import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import Role
from sqlalchemy.orm import DeclarativeBase
from typing import cast, Type

from orm_loader.tables.loadable_table import FileLoadableTableInterface
from orm_loader.tables.typing import FileTableProtocol


class Base(DeclarativeBase):
    pass


class ParquetTable(Base, FileLoadableTableInterface):
    __tablename__ = "parquet_table"
    __table_args__ = {"schema": Role.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    value: so.Mapped[int] = so.mapped_column(sa.Integer, nullable=False)


_ParquetTable = cast(Type[FileTableProtocol], ParquetTable)


def test_parquet_loader(sqlite_session, sqlite_engine, tmp_path):
    Base.metadata.create_all(sqlite_engine)

    df = pd.DataFrame(
        [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
        ]
    )
    table = pa.Table.from_pandas(df)
    path = tmp_path / "parquet_table.parquet"
    pq.write_table(table, path)

    inserted = _ParquetTable.load_file(
        sqlite_session,
        path,
        dedupe=False,
    )
    sqlite_session.commit()

    assert inserted == 2


class NullSentinelTable(Base, FileLoadableTableInterface):
    """Nullable text columns, for checking that marker-like text survives a Parquet load."""

    __tablename__ = "null_sentinel_table"
    __table_args__ = {"schema": Role.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str | None] = so.mapped_column(sa.String, nullable=True)
    role: so.Mapped[str | None] = so.mapped_column(sa.String, nullable=True)


_NullSentinelTable = cast(Type[FileTableProtocol], NullSentinelTable)

_BLANKS = ["  ", ""]
_MARKER_TEXT = ["NULL", "null", "n/a", "N/A", "NaN", "none", "None"]


def test_parquet_keeps_marker_text_and_nulls_blanks(sqlite_session, sqlite_engine, tmp_path):
    Base.metadata.create_all(sqlite_engine)

    values = _MARKER_TEXT + _BLANKS
    rows = [{"id": i, "name": v, "role": None} for i, v in enumerate(values, start=1)]
    rows.append({"id": 99, "name": " keep me ", "role": None})
    path = tmp_path / "null_sentinel_table.parquet"
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(rows)), path)

    _NullSentinelTable.load_file(sqlite_session, path, dedupe=False)
    sqlite_session.commit()

    stored = dict(sqlite_session.execute(sa.select(NullSentinelTable.id, NullSentinelTable.name)).all())
    assert [stored[i] for i in range(1, len(values) + 1)] == _MARKER_TEXT + [None] * len(_BLANKS)
    assert stored[99] == "keep me"
