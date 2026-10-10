"""Tests split CDM/vocab connection instances.
"""

from __future__ import annotations

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import CDMDatabaseConfig, ConnectionConfig, Resolver, StackConfig
from oa_configurator import Role as SchemaRole
from sqlalchemy.exc import UnboundExecutionError

from orm_loader.backends import staging_schema_claim
from orm_loader.loaders.loader_interface import PandasLoader
from tests.conftest import schema_scoped_session
from tests.models import SimpleTable, VocabSchemaTable


def test_vocab_csv_load_routes_every_session_bind_to_vocab_file(tmp_path):
    resolver = Resolver(
        StackConfig.for_session(
            connections={
                "primary": ConnectionConfig(
                    dialect="sqlite", database_name=str(tmp_path / "primary.db")
                ),
                "vocab": ConnectionConfig(
                    dialect="sqlite", database_name=str(tmp_path / "vocab.db")
                ),
            },
            databases={
                "test_cdm": CDMDatabaseConfig(connection="primary", vocab_connection="vocab")
            },
        )
    )
    resolved = resolver.resolve_database("test_cdm")
    primary_engine, vocab_engine = resolved.create_engines(
        schema_claims=[staging_schema_claim()]
    )
    VocabSchemaTable.__table__.create(vocab_engine)

    class MapperRoutedSession(so.Session):
        def __init__(self):
            super().__init__()
            self._last_mapper_engine = None

        def get_bind(self, mapper=None, clause=None, **kwargs):
            if mapper is None and clause is None:
                raise UnboundExecutionError("a mapper or clause is required")
            if mapper is not None:
                engine = vocab_engine if mapper.__table__.schema == SchemaRole.VOCAB else primary_engine
                self._last_mapper_engine = engine
                return engine
            table = getattr(clause, "table", None)
            if table is not None and table.schema in {SchemaRole.PRIMARY, SchemaRole.VOCAB}:
                return vocab_engine if table.schema == SchemaRole.VOCAB else primary_engine
            return self._last_mapper_engine or primary_engine

    csv_path = tmp_path / "test_vocab_role_table.csv"
    pd.DataFrame([{"id": 7, "name": "vocab-row"}]).to_csv(csv_path, index=False, sep="\t")
    session = MapperRoutedSession()
    try:
        VocabSchemaTable.load_csv(
            session,
            csv_path,
            merge_strategy="insert_if_empty",
            dedupe=False,
            loader=PandasLoader(),
            staging_schema_tag=staging_schema_claim().schema_tag,
        )
        session.commit()
    finally:
        session.close()

    with vocab_engine.connect() as connection:
        rows = connection.execute(
            sa.select(VocabSchemaTable.id, VocabSchemaTable.name)
        ).all()
    assert rows == [(7, "vocab-row")]
    assert not sa.inspect(primary_engine).has_table("test_vocab_role_table")
    primary_engine.dispose()
    vocab_engine.dispose()


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
