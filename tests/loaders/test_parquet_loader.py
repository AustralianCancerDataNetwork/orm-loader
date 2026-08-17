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


class NullSentinelTable(Base, CSVLoadableTableInterface):
    """Nullable text/number columns, for checking that the Arrow casting path
    treats text null sentinels the same way the scalar path does."""

    __tablename__ = "null_sentinel_table"

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str | None] = so.mapped_column(sa.String, nullable=True)
    role: so.Mapped[str | None] = so.mapped_column(sa.String, nullable=True)


_NullSentinelTable = cast(Type[CSVTableProtocol], NullSentinelTable)

_SENTINELS = ["NULL", "null", " NULL ", "n/a", "N/A", "NaN", "none", "None", "  ", ""]


def test_arrow_path_normalises_text_null_sentinels(session, engine, tmp_path):
    # cast_scalar normalises these before dispatching; without the same
    # treatment here the plain String branch wrote the literal text into
    # staging, so the same file loaded via the two loaders disagreed.
    Base.metadata.create_all(engine)

    rows = [{"id": i, "name": s, "role": None} for i, s in enumerate(_SENTINELS, start=1)]
    rows.append({"id": 99, "name": " keep me ", "role": None})
    path = tmp_path / "null_sentinel_table.parquet"
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(rows)), path)

    _NullSentinelTable.load_csv(session, path, loader=ParquetLoader(), dedupe=False)
    session.commit()

    stored = dict(session.execute(sa.select(NullSentinelTable.id, NullSentinelTable.name)).all())
    assert [stored[i] for i in range(1, len(_SENTINELS) + 1)] == [None] * len(_SENTINELS)
    assert stored[99] == "keep me"


def test_arrow_and_pandas_paths_agree_on_null_sentinels(session, engine, tmp_path):
    from orm_loader.loaders.data.converters import cast_arrow_column, cast_scalar

    col = NullSentinelTable.__table__.c.name
    for raw in _SENTINELS + [" keep me ", "plain"]:
        arrow = cast_arrow_column(pa.array([raw], type=pa.string()), col)[0].as_py()
        scalar = cast_scalar(raw, col.type)
        assert arrow == scalar, f"{raw!r}: arrow={arrow!r} scalar={scalar!r}"


def test_arrow_column_rule_does_not_count_nulls_as_cast_failures(session, engine, tmp_path):
    # The override branch resolved these to None either way, but recorded each
    # one as a failure -- warning about rows that are simply null.
    from enum import Enum

    from orm_loader.loaders.data.converters import (
        _COLUMN_CAST_RULES,
        cast_arrow_column,
        register_column_cast_rule,
    )
    from orm_loader.loaders.data_classes import TableCastingStats

    class Role(str, Enum):
        FIRST_AUTHOR = "first author"

    try:
        register_column_cast_rule("null_sentinel_table", "role", enum_type=Role)
        stats = TableCastingStats(table_name="null_sentinel_table")
        arr = cast_arrow_column(
            pa.array(_SENTINELS + ["first author", "BOGUS"], type=pa.string()),
            NullSentinelTable.__table__.c.role,
            stats=stats,
        )
        assert arr.to_pylist() == [None] * len(_SENTINELS) + ["FIRST_AUTHOR", None]
        # Only the genuinely unresolvable value is a failure.
        assert stats.total_failures == 1
        assert stats.columns["role"].examples == ["BOGUS"]
    finally:
        _COLUMN_CAST_RULES.clear()


def test_arrow_path_nulls_float_nan(session, engine, tmp_path):
    from orm_loader.loaders.data.converters import cast_arrow_column

    class FloatTable(Base, CSVLoadableTableInterface):
        __tablename__ = "float_sentinel_table"
        id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
        score: so.Mapped[float | None] = so.mapped_column(sa.Float, nullable=True)

    arr = cast_arrow_column(
        pa.array([1.5, float("nan"), None], type=pa.float64()),
        FloatTable.__table__.c.score,
    )
    assert arr.to_pylist() == [1.5, None, None]
