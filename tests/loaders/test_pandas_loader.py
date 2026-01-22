from pathlib import Path
import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.tables import CSVLoadableTableInterface
from orm_loader.loaders.data_classes import LoaderContext
import pandas as pd
from orm_loader.loaders.loader_interface import PandasLoader

Base = so.declarative_base()

class TestPandasLoader(CSVLoadableTableInterface, Base):
    __tablename__ = "test_pandas_loader"
    id = sa.Column(sa.Integer, primary_key=True)
    value = sa.Column(sa.Integer, nullable=False)

def test_loader_context_fields(session):

    ctx = LoaderContext(
        tableclass=TestPandasLoader,
        session=session,
        path=Path("file.csv"),
        staging_table=TestPandasLoader.__table__,
        chunksize=100,
        normalise=False,
        dedupe=True,
    )

    assert ctx.chunksize == 100
    assert ctx.normalise is False
    assert ctx.dedupe is True


def test_pandas_dedupe_internal(session, tmp_path):
    df = pd.DataFrame({
        "id": [1, 1, 2],
        "value": [10, 10, 20],
    })

    ctx = LoaderContext(
        tableclass=TestPandasLoader,
        session=session,
        path=Path("file.csv"),
        staging_table=TestPandasLoader.__table__,
        chunksize=100,
        normalise=False,
        dedupe=True,
    )

    out = PandasLoader.dedupe(df, ctx)
    assert len(out) == 2
