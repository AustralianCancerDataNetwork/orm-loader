"""End-to-end proof that load_csv() respects schema_translate_map with no
caller-side workaround. This is the actual regression test for the bug this
whole plan exists to fix, distinct from the backend unit tests in
tests/backends/, which exercise the merge methods directly but never against
a genuinely non-default schema.

Only Postgres is covered here. SQLite has no real schema concept (confirmed
in the plan's own audit), so there is no non-default-schema behavior to
regress there; SQLite's own dialect-specific correctness (the
postgresql.insert() vs sqlite.insert() upsert constructor split in
particular) is already covered by tests/backends/test_sqlite_backend.py and
the default-schema tests in test_loader_e2e.py.

Not create_mock_engine: MockConnection.schema_for_object ignores
schema_translate_map entirely, which would make this test pass whether or
not translation actually works. Real Postgres, via pg_db.
"""

from __future__ import annotations

import pandas as pd
import sqlalchemy as sa
from oa_configurator import Role as SchemaRole

from orm_loader.backends import STAGING_SCHEMA
from orm_loader.loaders.loader_interface import PandasLoader

from tests.conftest import schema_scoped_session
from tests.models import SimpleTable, VocabSchemaTable


def test_load_csv_respects_non_default_schema_end_to_end(pg_db, tmp_path):
    with schema_scoped_session(pg_db, SimpleTable.__table__, prefix="loader_primary") as (scoped, session):
        schema = scoped.schemas[SchemaRole.PRIMARY]
        csv_path = tmp_path / "test_table.csv"
        pd.DataFrame(
            [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}, {"id": 3, "name": "gamma"}]
        ).to_csv(csv_path, index=False, sep="\t")

        inserted = SimpleTable.load_csv(
            session, csv_path, dedupe=False, loader=PandasLoader(), staging_schema=STAGING_SCHEMA
        )
        session.commit()

        assert inserted == 3

        with scoped.engine.connect() as conn:
            # Read back through the schema-qualified name directly, not through
            # schema_translate_map, to prove the rows are really there.
            rows = conn.execute(
                sa.text(f'SELECT id, name FROM "{schema}"."test_table" ORDER BY id')
            ).fetchall()
            # Nothing leaked into public: raw text() bypassing
            # schema_translate_map would resolve through search_path instead.
            leaked = conn.execute(sa.text("SELECT to_regclass('public.test_table')")).scalar()
        assert rows == [(1, "alpha"), (2, "beta"), (3, "gamma")]
        assert leaked is None


def test_replace_merge_respects_non_default_schema_end_to_end(pg_db, tmp_path):
    """A second load_csv() call with merge_strategy="replace" against the
    same non-default schema. Proves the merge path itself, not just the
    initial insert-if-empty fast path, qualifies correctly."""
    with schema_scoped_session(pg_db, SimpleTable.__table__, prefix="loader_replace") as (scoped, session):
        schema = scoped.schemas[SchemaRole.PRIMARY]

        def _write_and_load(rows: list[dict], path_name: str) -> int:
            path = tmp_path / path_name
            pd.DataFrame(rows).to_csv(path, index=False, sep="\t")
            return SimpleTable.load_csv(
                session,
                path,
                dedupe=False,
                loader=PandasLoader(),
                merge_strategy="replace",
                staging_schema=STAGING_SCHEMA,
            )

        _write_and_load([{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}], "test_table.csv")
        session.commit()
        _write_and_load([{"id": 1, "name": "alpha-updated"}], "test_table.csv")
        session.commit()

        with scoped.engine.connect() as conn:
            rows = conn.execute(
                sa.text(f'SELECT id, name FROM "{schema}"."test_table" ORDER BY id')
            ).fetchall()
        assert rows == [(1, "alpha-updated"), (2, "beta")]


def test_load_csv_respects_non_primary_schema_tag_end_to_end(pg_db, tmp_path):
    """Checks that the schema tag is correctly derived from the table's own
    schema (table.schema) for a non-primary tag."""
    with schema_scoped_session(
        pg_db, VocabSchemaTable.__table__, prefix="loader_vocab", split_roles=[SchemaRole.VOCAB]
    ) as (scoped, session):
        primary_schema = scoped.schemas[SchemaRole.PRIMARY]
        vocab_schema = scoped.schemas[SchemaRole.VOCAB]
        csv_path = tmp_path / "test_vocab_role_table.csv"
        pd.DataFrame([{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]).to_csv(
            csv_path, index=False, sep="\t"
        )

        inserted = VocabSchemaTable.load_csv(
            session, csv_path, dedupe=False, loader=PandasLoader(), staging_schema=STAGING_SCHEMA
        )
        session.commit()

        assert inserted == 2

        with scoped.engine.connect() as conn:
            rows = conn.execute(
                sa.text(f'SELECT id, name FROM "{vocab_schema}"."test_vocab_role_table" ORDER BY id')
            ).fetchall()
            leaked = conn.execute(
                sa.text(f"SELECT to_regclass('{primary_schema}.test_vocab_role_table')")
            ).scalar()
        assert rows == [(1, "alpha"), (2, "beta")]
        assert leaked is None
