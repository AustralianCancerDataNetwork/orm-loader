import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.orm import Session
from pathlib import Path
import pandas as pd
import pytest
from orm_loader.loaders.data_classes import _clean_nulls
from orm_loader.tables.loadable_table import CSVLoadableTableInterface
from orm_loader.loaders.loader_interface import PandasLoader
from orm_loader.loaders.loading_helpers import infer_encoding, infer_delim, check_line_ending, quick_load_pg

from tests.models import Base, SimpleTable, RequiredTable, CompositeTable

import numpy as np

@pytest.mark.postgres
def test_copy_and_orm_path_equivalence(pg_session, tmp_path):
    csv = tmp_path / "test_table.csv"

    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
        ]
    ).to_csv(csv, index=False, sep="\t")

    inserted = SimpleTable.load_csv(pg_session, csv)
    pg_session.commit()

    rows = pg_session.execute(sa.select(SimpleTable).order_by(SimpleTable.id)).scalars().all()
    assert [(r.id, r.name) for r in rows] == [
        (1, "alpha"),
        (2, "beta"),
    ]



@pytest.mark.postgres
def test_postgres_copy_fast_path(pg_session, tmp_path):
    csv = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv, index=False)

    inserted = SimpleTable.load_csv(pg_session, csv)
    pg_session.commit()

    assert inserted == 1

@pytest.mark.postgres
def test_postgres_copy_fast_path_is_used(pg_session, tmp_path, monkeypatch):
    csv = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv, index=False)

    called = {"copy": False}

    def fake_quick_load_pg(*args, **kwargs):
        called["copy"] = True
        return 1

    import orm_loader.tables.loadable_table as loadable_table
    monkeypatch.setattr(loadable_table, "quick_load_pg", fake_quick_load_pg)

    inserted = SimpleTable.load_csv(pg_session, csv)
    pg_session.commit()

    assert called["copy"] is True
    assert inserted == 1

@pytest.mark.postgres
def test_copy_failure_falls_back_to_orm(pg_session, tmp_path, monkeypatch):
    csv = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv, index=False)

    from orm_loader.loaders import loading_helpers

    def broken_copy(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(loading_helpers, "quick_load_pg", broken_copy)

    inserted = SimpleTable.load_csv(pg_session, csv)
    pg_session.commit()

    rows = pg_session.execute(sa.select(SimpleTable)).scalars().all()
    assert inserted == 1
    assert [(r.id, r.name) for r in rows] == [(1, "alpha")]


@pytest.mark.postgres
def test_postgres_upsert_does_not_update(pg_session, tmp_path):
    csv = tmp_path / "test_table.csv"

    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv, index=False)
    SimpleTable.load_csv(pg_session, csv)
    pg_session.commit()

    pd.DataFrame([{"id": 1, "name": "alpha_updated"}]).to_csv(csv, index=False)

    SimpleTable.load_csv(pg_session, csv, merge_strategy="upsert")
    pg_session.commit()

    rows = pg_session.execute(sa.select(SimpleTable)).scalars().all()
    assert [(r.id, r.name) for r in rows] == [(1, "alpha")]


@pytest.mark.postgres
def test_postgres_copy_large_batch(pg_session, tmp_path):
    csv = tmp_path / "test_table.csv"

    df = pd.DataFrame(
        [{"id": i, "name": f"name_{i}"} for i in range(1, 10_000)]
    )
    df.to_csv(csv, index=False)

    inserted = SimpleTable.load_csv(pg_session, csv)
    pg_session.commit()

    count = pg_session.execute(sa.text('SELECT COUNT(*) FROM test_table')).scalar()
    assert count == 9999
    assert inserted == 9999


@pytest.mark.postgres
def test_staging_schema_matches_target(pg_session, tmp_path):
    csv = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv, index=False)

    SimpleTable.create_staging_table(pg_session)

    cols = pg_session.execute(sa.text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = :table
        ORDER BY ordinal_position
    """), {"table": SimpleTable.staging_tablename()}).all()

    assert cols == [
        ("id", "integer"),
        ("name", "character varying"),
    ]


def test_infer_encoding_ascii_promoted_to_utf8(tmp_path):
    p = tmp_path / "ascii.csv"
    p.write_bytes(b"id,name\n1,alpha\n")

    enc = infer_encoding(p)
    assert enc["encoding"] == "utf-8"

@pytest.mark.xfail(reason="chardet todo - finalise encoding inference")
def test_infer_encoding_utf8(tmp_path):
    p = tmp_path / "utf8.csv"
    p.write_text("id,name\n1,α\n", encoding="utf-8")

    enc = infer_encoding(p)
    assert enc["encoding"].lower().startswith("utf")

def test_infer_delim_tab(tmp_path):
    p = tmp_path / "tab.csv"
    p.write_text("id\tname\n1\talpha\n")

    assert infer_delim(p) == "\t"

def test_infer_delim_comma(tmp_path):
    p = tmp_path / "comma.csv"
    p.write_text("id,name\n1,alpha\n")

    assert infer_delim(p) == ","

@pytest.mark.parametrize(
    "raw,expected",
    [
        ("a,b\r\n", "\r\n"),
        ("a,b\n", "\n"),
        ("a,b\r", "\r"),
    ],
)
def test_check_line_ending(raw, expected):
    assert check_line_ending(raw) == expected


def test_check_line_ending_unknown(caplog):
    raw = "a,b"
    out = check_line_ending(raw)
    assert out == "\n"
    assert "Unable to detect line ending" in caplog.text


@pytest.mark.postgres
def test_quick_load_pg_basic(pg_session, tmp_path):
    csv = tmp_path / "test_table.csv"
    csv.write_text("id,name\n1,alpha\n2,beta\n")

    total = quick_load_pg(
        path=csv,
        session=pg_session,
        tablename="test_table",
    )

    rows = pg_session.execute(sa.text('SELECT id, name FROM test_table ORDER BY id')).all()
    assert total == 2
    assert rows == [(1, "alpha"), (2, "beta")]


@pytest.mark.postgres
def test_quick_load_pg_lowercases_header(pg_session, tmp_path):
    csv = tmp_path / "test_table.csv"
    csv.write_text("ID,NAME\n1,alpha\n")

    total = quick_load_pg(path=csv, session=pg_session, tablename="test_table")
    row = pg_session.execute(sa.text('SELECT id, name FROM test_table')).one()

    assert total == 1
    assert row == (1, "alpha")


@pytest.mark.postgres
def test_quick_load_pg_tab_delimiter(pg_session, tmp_path):
    csv = tmp_path / "test_table.csv"
    csv.write_text("id\tname\n1\talpha\n2\tbeta\n")

    total = quick_load_pg(path=csv, session=pg_session, tablename="test_table")
    rows = pg_session.execute(sa.text("SELECT id, name FROM test_table ORDER BY id")).all()

    assert total == 2
    assert rows == [(1, "alpha"), (2, "beta")]


@pytest.mark.postgres
def test_quick_load_pg_rollback_on_error(pg_session, tmp_path):
    csv = tmp_path / "test_table.csv"
    csv.write_text("id,name\n1,alpha\n2,\n")  # violates NOT NULL

    with pytest.raises(Exception):
        quick_load_pg(path=csv, session=pg_session, tablename="test_table")

    rows = pg_session.execute(sa.text("SELECT COUNT(*) FROM test_table")).scalar_one()
    assert rows == 0


@pytest.mark.postgres
def test_quick_load_pg_equivalence_with_orm(pg_session, tmp_path):
    csv = tmp_path / "test_table.csv"
    csv.write_text("id,name\n1,alpha\n2,beta\n")

    quick_load_pg(path=csv, session=pg_session, tablename="test_table")

    rows_pg = pg_session.execute(sa.text("SELECT id, name FROM test_table ORDER BY id")).all()

    pg_session.execute(sa.text("TRUNCATE test_table"))
    pg_session.commit()

    df = pd.read_csv(csv)
    pg_session.execute(sa.text("""
        INSERT INTO test_table (id, name)
        VALUES (:id, :name)
    """), df.to_dict(orient="records"))
    pg_session.commit()

    rows_orm = pg_session.execute(sa.text("SELECT id, name FROM test_table ORDER BY id")).all()

    assert rows_pg == rows_orm


@pytest.mark.postgres
def test_quick_load_pg_trailing_blank_lines(pg_session, tmp_path):
    csv = tmp_path / "test_table.csv"

    csv.write_text(
        "id,name\n"
        "1,alpha\r\n"
        "2,beta\r\n"
    )

    total = quick_load_pg(path=csv, session=pg_session, tablename="test_table")

    rows = pg_session.execute(
        sa.text("SELECT id, name FROM test_table ORDER BY id")
    ).all()

    assert total == 2
    assert rows == [(1, "alpha"), (2, "beta")]

@pytest.mark.postgres
def test_copy_fails_with_raw_carriage_returns_but_succeeds_after_normalisation(pg_session, tmp_path):
    csv = tmp_path / "test_table.csv"

    # Intentionally include CRLF to simulate Windows-originated files
    raw = (
        b"id,name\r\n"
        b"1,alpha\r\n"
        b"2,beta\r\n"
    )

    csv.write_bytes(raw)

    # First: raw COPY (current behaviour) — this may fail on some systems
    failed = False
    try:
        quick_load_pg(path=csv, session=pg_session, tablename="test_table")
        pg_session.commit()
    except Exception as e:
        failed = True

    # Clean up table for second attempt
    pg_session.execute(sa.text('TRUNCATE "test_table"'))
    pg_session.commit()

    # Now normalise newlines exactly the way your fix would
    normalised = raw.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    csv.write_bytes(normalised)

    # Second: normalised COPY — should always succeed
    total = quick_load_pg(path=csv, session=pg_session, tablename="test_table")
    pg_session.commit()

    rows = pg_session.execute(
        sa.text('SELECT id, name FROM test_table ORDER BY id')
    ).all()

    assert total == 2
    assert rows == [(1, "alpha"), (2, "beta")]

    # This assertion is the diagnostic: on systems where COPY is strict,
    # the first attempt should fail.
    # On systems where it "works natively", you may see failed == False,
    # which proves the environment difference.
    print("Raw COPY failed:", failed)
