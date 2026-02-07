from pathlib import Path
import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.loaders.data_classes import LoaderContext
import pandas as pd
from orm_loader.loaders.loader_interface import PandasLoader
from tests.models import PandasLoaderTable

def test_loader_context_fields(session):

    ctx = LoaderContext(
        tableclass=PandasLoaderTable,
        session=session,
        path=Path("file.csv"),
        staging_table=PandasLoaderTable.__table__,
        chunksize=100,
        normalise=False,
        dedupe=True,
    )

    assert ctx.chunksize == 100
    assert ctx.normalise is False
    assert ctx.dedupe is True

def test_pandas_loader_case_insensitive_headers(tmp_path, session):
    """
    PandasLoader should accept mixed-case CSV headers and map them
    correctly to lowercase model columns.
    """

    csv = tmp_path / "test_case_headers.csv"
    csv.write_text(
        "ID,Value\n"
        "1,alpha\n"
        "2,beta\n"
    )

    ctx = LoaderContext(
        tableclass=PandasLoaderTable,
        session=session,
        path=csv,
        staging_table=PandasLoaderTable.__table__,
        chunksize=100,
        normalise=True,
        dedupe=True,
    )

    n = PandasLoader.orm_file_load(ctx)

    rows = session.execute(
        sa.text('SELECT id, value FROM test_pandas_loader ORDER BY id')
    ).all()

    assert n == 2
    assert rows == [(1, "alpha"), (2, "beta")]


def test_pandas_dedupe_internal(session, tmp_path):
    df = pd.DataFrame({
        "id": [1, 1, 2],
        "value": [10, 10, 20],
    })

    ctx = LoaderContext(
        tableclass=PandasLoaderTable,
        session=session,
        path=Path("file.csv"),
        staging_table=PandasLoaderTable.__table__,
        chunksize=100,
        normalise=False,
        dedupe=True,
    )

    out = PandasLoader.dedupe(df, ctx)
    assert len(out) == 2
