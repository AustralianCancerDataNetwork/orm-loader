"""Tests split CDM/vocab connection instances.
"""

from __future__ import annotations

import pandas as pd
import sqlalchemy as sa
from oa_configurator import Role as SchemaRole

from orm_loader.loaders.loader_interface import PandasLoader

from tests.conftest import schema_scoped_session
from tests.models import SimpleTable, VocabSchemaTable


def test_load_csv_across_two_genuinely_separate_connections(pg_db, session, tmp_path):
    """``pg_db`` (real Postgres, primary connection) and ``session`` (real
    SQLite, vocab connection, via the module-level ``engine``/``session``
    fixtures) are two entirely different engines against two entirely
    different database systems."""
    with schema_scoped_session(pg_db, SimpleTable.__table__, prefix="loader_split") as (scoped, primary_session):
        primary_schema = scoped.schemas[SchemaRole.PRIMARY]
        primary_csv = tmp_path / "test_table.csv"
        pd.DataFrame([{"id": 1, "name": "primary-alpha"}]).to_csv(primary_csv, index=False, sep="\t")

        vocab_csv = tmp_path / "test_vocab_role_table.csv"
        pd.DataFrame([{"id": 1, "name": "vocab-alpha"}]).to_csv(vocab_csv, index=False, sep="\t")

        # Interleaved on purpose: primary, then vocab, then primary again, so a
        # module-level cache keyed wrong (or reused across calls) would surface
        # as data landing in the wrong database.
        SimpleTable.load_csv(primary_session, primary_csv, dedupe=False, loader=PandasLoader())
        primary_session.commit()

        VocabSchemaTable.load_csv(session, vocab_csv, dedupe=False, loader=PandasLoader())
        session.commit()

        with scoped.engine.connect() as conn:
            primary_rows = conn.execute(
                sa.text(f'SELECT id, name FROM "{primary_schema}"."test_table"')
            ).fetchall()
            # Neither database saw the other's table/data at all.
            leaked_vocab_table_in_pg = conn.execute(
                sa.text(f"SELECT to_regclass('{primary_schema}.test_vocab_role_table')")
            ).scalar()
        assert primary_rows == [(1, "primary-alpha")]
        assert leaked_vocab_table_in_pg is None

        vocab_rows = session.execute(
            sa.select(VocabSchemaTable).order_by(VocabSchemaTable.id)
        ).scalars().all()
        assert [(r.id, r.name) for r in vocab_rows] == [(1, "vocab-alpha")]
