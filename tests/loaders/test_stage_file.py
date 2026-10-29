from pathlib import Path

import sqlalchemy as sa

from orm_loader.loaders import LoaderContext, stage_file
from tests.models import FileLoaderTable


def test_loader_context_fields(sqlite_session):

    ctx = LoaderContext(
        tableclass=FileLoaderTable,
        session=sqlite_session,
        path=Path("file.csv"),
        staging_table=FileLoaderTable.__table__,
        chunksize=100,
        normalise=False,
        dedupe=True,
    )

    assert ctx.chunksize == 100
    assert ctx.normalise is False
    assert ctx.dedupe is True

def test_stage_file_case_insensitive_headers(tmp_path, sqlite_session):
    """
    The loader should accept mixed-case CSV headers and map them
    correctly to lowercase model columns.
    """

    csv = tmp_path / "test_case_headers.csv"
    csv.write_text(
        "ID,Value\n"
        "1,alpha\n"
        "2,beta\n"
    )

    ctx = LoaderContext(
        tableclass=FileLoaderTable,
        session=sqlite_session,
        path=csv,
        staging_table=FileLoaderTable.__table__,
        chunksize=100,
        normalise=True,
        dedupe=True,
    )

    n = stage_file(ctx, ctx.tableclass._select_reader(ctx))

    rows = sqlite_session.execute(
        sa.text('SELECT id, value FROM test_file_loader ORDER BY id')
    ).all()

    assert n == 2
    assert rows == [(1, "alpha"), (2, "beta")]


def test_stage_file_literal_mode_preserves_quotes(tmp_path, sqlite_session):
    """
    The loader must honour quote_mode. Under 'literal' it uses
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
        tableclass=FileLoaderTable,
        session=sqlite_session,
        path=csv,
        staging_table=FileLoaderTable.__table__,
        chunksize=100,
        normalise=True,
        dedupe=True,
        quote_mode="literal",
    )

    n = stage_file(ctx, ctx.tableclass._select_reader(ctx))

    rows = sqlite_session.execute(
        sa.text('SELECT id, value FROM test_file_loader ORDER BY id')
    ).all()

    assert n == 2
    assert rows == [(1, '"hello"'), (2, '"world"')]


def test_stage_file_csv_mode_strips_quotes(tmp_path, sqlite_session):
    """
    Contrast with the literal case above: the same file under 'csv' mode reads
    double quotes as field quoting, so the wrapping quotes are consumed.
    """

    csv = tmp_path / "quoted.csv"
    csv.write_text(
        'id,value\n'
        '1,"hello"\n'
        '2,"world"\n'
    )

    ctx = LoaderContext(
        tableclass=FileLoaderTable,
        session=sqlite_session,
        path=csv,
        staging_table=FileLoaderTable.__table__,
        chunksize=100,
        normalise=True,
        dedupe=True,
        quote_mode="csv",
    )

    n = stage_file(ctx, ctx.tableclass._select_reader(ctx))

    rows = sqlite_session.execute(
        sa.text('SELECT id, value FROM test_file_loader ORDER BY id')
    ).all()

    assert n == 2
    assert rows == [(1, "hello"), (2, "world")]


def test_stage_file_by_delimiter_tab_preserves_quotes(tmp_path, sqlite_session):
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
        tableclass=FileLoaderTable,
        session=sqlite_session,
        path=tsv,
        staging_table=FileLoaderTable.__table__,
        chunksize=100,
        normalise=True,
        dedupe=True,
        quote_mode="by_delimiter",
    )

    n = stage_file(ctx, ctx.tableclass._select_reader(ctx))

    rows = sqlite_session.execute(
        sa.text('SELECT id, value FROM test_file_loader ORDER BY id')
    ).all()

    assert n == 2
    assert rows == [(1, '"hi"'), (2, "bye")]


def test_file_without_data_rows_loads_nothing_whatever_its_header(tmp_path, sqlite_session):
    for text in ("stub\n", "id,value\n", ""):
        csv = tmp_path / "no_rows.csv"
        csv.write_text(text)
        ctx = LoaderContext(
            tableclass=FileLoaderTable,
            session=sqlite_session,
            path=csv,
            staging_table=FileLoaderTable.__table__,
        )
        assert stage_file(ctx, ctx.tableclass._select_reader(ctx)) == 0
        assert ctx.report.rows_read == 0
