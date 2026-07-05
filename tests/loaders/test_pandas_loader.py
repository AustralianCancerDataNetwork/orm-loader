from pathlib import Path
import sqlalchemy as sa
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


def test_pandas_loader_literal_mode_preserves_quotes(tmp_path, session):
    """
    The pandas fallback must honour quote_mode. Under 'literal' it uses
    QUOTE_NONE, so double-quotes are literal data and are preserved verbatim
    rather than being stripped as RFC-4180 field wrappers.
    """

    csv = tmp_path / "quoted.csv"
    csv.write_text(
        'id,value\n'
        '1,"hello"\n'
        '2,"world"\n'
    )

    ctx = LoaderContext(
        tableclass=PandasLoaderTable,
        session=session,
        path=csv,
        staging_table=PandasLoaderTable.__table__,
        chunksize=100,
        normalise=True,
        dedupe=True,
        quote_mode="literal",
    )

    n = PandasLoader.orm_file_load(ctx)

    rows = session.execute(
        sa.text('SELECT id, value FROM test_pandas_loader ORDER BY id')
    ).all()

    assert n == 2
    assert rows == [(1, '"hello"'), (2, '"world"')]


def test_pandas_loader_csv_mode_strips_quotes(tmp_path, session):
    """
    Contrast with the literal case above: the same file under 'csv' mode uses
    QUOTE_MINIMAL, so the wrapping quotes are consumed. Proves quote_mode
    actually flows through the fallback rather than being ignored.
    """

    csv = tmp_path / "quoted.csv"
    csv.write_text(
        'id,value\n'
        '1,"hello"\n'
        '2,"world"\n'
    )

    ctx = LoaderContext(
        tableclass=PandasLoaderTable,
        session=session,
        path=csv,
        staging_table=PandasLoaderTable.__table__,
        chunksize=100,
        normalise=True,
        dedupe=True,
        quote_mode="csv",
    )

    n = PandasLoader.orm_file_load(ctx)

    rows = session.execute(
        sa.text('SELECT id, value FROM test_pandas_loader ORDER BY id')
    ).all()

    assert n == 2
    assert rows == [(1, "hello"), (2, "world")]


def test_pandas_loader_by_delimiter_tab_preserves_quotes(tmp_path, session):
    """
    'by_delimiter' derives the mode from the delimiter alone: a tab-delimited
    file resolves to 'literal', so embedded double-quotes are kept as data.
    """

    tsv = tmp_path / "tab.tsv"
    tsv.write_text(
        'id\tvalue\n'
        '1\t"hi"\n'
        '2\tbye\n'
    )

    ctx = LoaderContext(
        tableclass=PandasLoaderTable,
        session=session,
        path=tsv,
        staging_table=PandasLoaderTable.__table__,
        chunksize=100,
        normalise=True,
        dedupe=True,
        quote_mode="by_delimiter",
    )

    n = PandasLoader.orm_file_load(ctx)

    rows = session.execute(
        sa.text('SELECT id, value FROM test_pandas_loader ORDER BY id')
    ).all()

    assert n == 2
    assert rows == [(1, '"hi"'), (2, "bye")]


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
