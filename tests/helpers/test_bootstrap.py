from __future__ import annotations

import pytest
import sqlalchemy as sa
from oa_configurator import CDMDatabaseConfig, ConnectionConfig, Resolver

from orm_loader.helpers import bootstrap, create_db, create_tables


def _resolved(tmp_path, *, split: bool):
    connections = {
        "primary": ConnectionConfig(dialect="sqlite", database_name=str(tmp_path / "primary.db")),
    }
    vocab_connection = "primary"
    if split:
        connections["vocab"] = ConnectionConfig(
            dialect="sqlite", database_name=str(tmp_path / "vocab.db")
        )
        vocab_connection = "vocab"
    resolver = Resolver.from_active_config().with_overrides(
        connections=connections,
        databases={
            "test_cdm": CDMDatabaseConfig(connection="primary", vocab_connection=vocab_connection)
        },
    )
    return resolver.resolve_database("test_cdm")


def _engine():
    return sa.create_engine(
        "sqlite://",
        execution_options={"schema_translate_map": {"primary": None, "vocab": None}},
    )


def test_create_tables_colocated(tmp_path):
    resolved = _resolved(tmp_path, split=False)
    metadata = sa.MetaData()
    primary = sa.Table(
        "primary_table", metadata, sa.Column("id", sa.Integer, primary_key=True), schema="primary"
    )
    vocab = sa.Table(
        "vocab_table", metadata, sa.Column("id", sa.Integer, primary_key=True), schema="vocab"
    )
    engine = _engine()

    with engine.begin() as connection:
        create_tables(connection, [primary, vocab], resolved=resolved)

    assert sa.inspect(engine).has_table("primary_table")
    assert sa.inspect(engine).has_table("vocab_table")
    engine.dispose()


def test_create_tables_split_drops_cross_database_fk_and_keeps_local_fk(tmp_path):
    resolved = _resolved(tmp_path, split=True)
    metadata = sa.MetaData()
    parent = sa.Table(
        "parent", metadata, sa.Column("id", sa.Integer, primary_key=True), schema="primary"
    )
    child = sa.Table(
        "child",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("parent_id", sa.Integer, sa.ForeignKey("primary.parent.id")),
        sa.Column("remote_id", sa.Integer, sa.ForeignKey("vocab.remote.id")),
        schema="primary",
    )
    engine = _engine()

    with engine.begin() as connection:
        create_tables(connection, [parent, child], resolved=resolved)

    foreign_keys = sa.inspect(engine).get_foreign_keys("child")
    assert [fk["referred_table"] for fk in foreign_keys] == ["parent"]
    engine.dispose()


def test_create_tables_crossing_subset_resolves_local_fk_to_existing_table(tmp_path):
    resolved = _resolved(tmp_path, split=True)
    metadata = sa.MetaData()
    parent = sa.Table(
        "local_parent", metadata, sa.Column("id", sa.Integer, primary_key=True), schema="primary"
    )
    sa.Table("remote_parent", metadata, sa.Column("id", sa.Integer, primary_key=True), schema="vocab")
    child = sa.Table(
        "crossing_child",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("parent_id", sa.Integer, sa.ForeignKey("primary.local_parent.id")),
        sa.Column("remote_id", sa.Integer, sa.ForeignKey("vocab.remote_parent.id")),
        schema="primary",
    )
    engine = _engine()

    with engine.begin() as connection:
        parent.create(connection)
        create_tables(connection, [child], resolved=resolved)

    assert sa.inspect(engine).has_table("crossing_child")
    assert [fk["referred_table"] for fk in sa.inspect(engine).get_foreign_keys("crossing_child")] == [
        "local_parent"
    ]
    engine.dispose()


def test_create_tables_rejects_multiple_database_transactions(tmp_path):
    resolved = _resolved(tmp_path, split=True)
    metadata = sa.MetaData()
    primary = sa.Table("primary_table", metadata, sa.Column("id", sa.Integer), schema="primary")
    vocab = sa.Table("vocab_table", metadata, sa.Column("id", sa.Integer), schema="vocab")
    engine = _engine()

    with (
        engine.begin() as connection,
        pytest.raises(ValueError, match="share one database transaction"),
    ):
        create_tables(connection, [primary, vocab], resolved=resolved)

    engine.dispose()


def test_create_tables_crossing_path_accepts_non_base_metadata(tmp_path):
    resolved = _resolved(tmp_path, split=True)
    metadata = sa.MetaData()
    external = sa.Table(
        "external_table",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("remote_id", sa.Integer, sa.ForeignKey("vocab.remote.id")),
        schema="primary",
    )
    engine = _engine()

    with engine.begin() as connection:
        create_tables(connection, [external], resolved=resolved)

    assert sa.inspect(engine).has_table("external_table")
    assert sa.inspect(engine).get_foreign_keys("external_table") == []
    engine.dispose()


def test_create_db_rejects_one_bind_for_split_database(tmp_path):
    resolved = _resolved(tmp_path, split=True)
    engine = _engine()

    with pytest.raises(ValueError, match="one bind cannot host both databases"):
        create_db(resolved, bindable=engine)

    engine.dispose()


@pytest.mark.parametrize("entry", [create_db, bootstrap])
def test_bootstrap_entry_points_require_resolved_database(entry):
    engine = _engine()
    with pytest.raises(TypeError, match="requires a ResolvedDatabase"):
        entry(engine)
    engine.dispose()
