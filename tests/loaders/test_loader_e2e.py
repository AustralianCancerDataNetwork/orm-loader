from typing import Type, cast

import logging
import pandas as pd
import pytest
import sqlalchemy as sa
import sqlalchemy.event as sae
import sqlalchemy.orm as so
from oa_configurator import Role as SchemaRole

from orm_loader.backends import resolve_backend
from orm_loader.backends.base import DeferredForeignKeyViolationError
from orm_loader.helpers import IngestError, RequiredValueError
from orm_loader.tables.loadable_table import FileLoadableTableInterface
from orm_loader.tables.typing import FileTableProtocol
from tests.models import (
    Base,
    CompositeTable,
    CycleATable,
    EnumTable,
    RequiredTable,
    SimpleTable,
    VocabSchemaTable,
)

# Typed aliases: Pylance cannot verify SQLAlchemy metaclass-generated attrs
# satisfy FileTableProtocol structurally, so we cast once per class here.
_SimpleTable = cast(Type[FileTableProtocol], SimpleTable)
_RequiredTable = cast(Type[FileTableProtocol], RequiredTable)
_CompositeTable = cast(Type[FileTableProtocol], CompositeTable)
_EnumTable = cast(Type[FileTableProtocol], EnumTable)
_VocabSchemaTable = cast(Type[FileTableProtocol], VocabSchemaTable)
_CycleATable = cast(Type[FileTableProtocol], CycleATable)


def test_initial_file_load(sqlite_session, tmp_path):
    csv_path = tmp_path / "test_table.csv"

    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
            {"id": 3, "name": "gamma"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    inserted = _SimpleTable.load_file(
        sqlite_session,
        csv_path,
        dedupe=False,
    )
    sqlite_session.commit()

    assert inserted == 3

    rows = sqlite_session.execute(sa.select(SimpleTable).order_by(SimpleTable.id)).scalars().all()

    assert [(r.id, r.name) for r in rows] == [
        (1, "alpha"),
        (2, "beta"),
        (3, "gamma"),
    ]


def test_initial_file_load_for_a_non_primary_schema_table(sqlite_session, tmp_path):
    """SQLite has no real schema concept, so every Role folds to None
    on this connection (see oa_configurator's SQLiteTestStrategy).
    A VOCAB-tagged table's load path must not error out just because
    the table's declared schema tag differs from primary. This is the SQLite
    counterpart to test_schema_translate_map.py's Postgres-only, non-primary-
    schema-tag coverage."""
    csv_path = tmp_path / "test_vocab_role_table.csv"

    pd.DataFrame(
        [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]
    ).to_csv(csv_path, index=False, sep="\t")

    inserted = _VocabSchemaTable.load_file(
        sqlite_session, csv_path, dedupe=False
    )
    sqlite_session.commit()

    assert inserted == 2

    rows = sqlite_session.execute(
        sa.select(VocabSchemaTable).order_by(VocabSchemaTable.id)
    ).scalars().all()

    assert [(r.id, r.name) for r in rows] == [(1, "alpha"), (2, "beta")]


def test_replace_merge_strategy(sqlite_session, tmp_path):
    csv_path = tmp_path / "test_table.csv"

    # Initial load
    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
            {"id": 3, "name": "gamma"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    _SimpleTable.load_file(
        sqlite_session,
        csv_path,
        dedupe=False,
    )
    sqlite_session.commit()

    # Replace rows 2 and 3
    pd.DataFrame(
        [
            {"id": 2, "name": "beta_updated"},
            {"id": 3, "name": "gamma_updated"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    replaced = _SimpleTable.load_file(
        sqlite_session,
        csv_path,
        dedupe=False,
        merge_strategy="replace",
    )
    sqlite_session.commit()

    assert replaced == 2

    rows = sqlite_session.execute(sa.select(SimpleTable).order_by(SimpleTable.id)).scalars().all()

    assert [(r.id, r.name) for r in rows] == [
        (1, "alpha"),
        (2, "beta_updated"),
        (3, "gamma_updated"),
    ]


def test_empty_file_is_noop(sqlite_session, tmp_path):
    csv_path = tmp_path / "test_table.csv"
    csv_path.touch()

    inserted = _SimpleTable.load_file(
        sqlite_session,
        csv_path,
        dedupe=False,
    )
    sqlite_session.commit()

    assert inserted == 0

    rows = sqlite_session.execute(sa.select(SimpleTable)).scalars().all()
    assert rows == []


def test_required_column_violation_raises(sqlite_session, tmp_path):
    csv = tmp_path / "required_table.csv"
    pd.DataFrame(
        [
            {"id": 1, "name": "ok"},
            {"id": 2, "name": None},  # invalid
        ]
    ).to_csv(csv, index=False)

    with pytest.raises(RequiredValueError, match="name"):
        _RequiredTable.load_file(
            sqlite_session,
            csv,
            dedupe=False,
        )


def test_required_column_entirely_missing_raises(sqlite_session, tmp_path):
    # Distinct from test_required_column_violation_raises above: here
    # the required column isn't present with a bad value, it's absent from
    # the source file entirely. No merge strategy supports a partial-column
    # load, so this must raise up front rather than silently proceed to a
    # less diagnosable NOT NULL failure at insert time.
    csv = tmp_path / "required_table.csv"
    pd.DataFrame([{"id": 1}, {"id": 2}]).to_csv(csv, index=False)

    with pytest.raises(IngestError, match="required_table.*name"):
        _RequiredTable.load_file(
            sqlite_session,
            csv,
            dedupe=False,
        )


def test_composite_pk_dedup(sqlite_session, tmp_path):
    csv = tmp_path / "composite_table.csv"
    pd.DataFrame(
        [
            {"a": 1, "b": 1, "value": "x"},
            {"a": 1, "b": 1, "value": "x"},
            {"a": 1, "b": 2, "value": "y"},
        ]
    ).to_csv(csv, index=False)

    inserted = _CompositeTable.load_file(
        sqlite_session,
        csv,
        dedupe=True,
    )
    sqlite_session.commit()

    assert inserted == 2


@pytest.mark.parametrize(
    "merge_strategy,expected_rows,expected_inserted",
    [
        (
            "replace",
            [
                (1, "alpha"),
                (2, "beta_updated"),
                (3, "gamma_updated"),
            ],
            2,
        ),
        (
            "upsert",
            [
                (1, "alpha"),
                (2, "beta"),
                (3, "gamma"),
            ],
            2,
        ),
    ],
)
def test_merge_strategies(sqlite_session, tmp_path, merge_strategy, expected_rows, expected_inserted):
    csv_path = tmp_path / "test_table.csv"

    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
            {"id": 3, "name": "gamma"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    SimpleTable.load_file(sqlite_session, csv_path, dedupe=False)
    sqlite_session.commit()

    pd.DataFrame(
        [
            {"id": 2, "name": "beta_updated"},
            {"id": 3, "name": "gamma_updated"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    inserted = SimpleTable.load_file(
        sqlite_session,
        csv_path,
        dedupe=False,
        merge_strategy=merge_strategy,
    )
    sqlite_session.commit()

    assert inserted == expected_inserted

    rows = (
        sqlite_session.execute(sa.select(SimpleTable).order_by(SimpleTable.id, SimpleTable.name))
        .scalars()
        .all()
    )

    assert [(r.id, r.name) for r in rows] == expected_rows


def test_a_load_resolves_its_backend_once(sqlite_session, tmp_path, monkeypatch):
    import orm_loader.tables.loadable_table as loadable_table

    calls = []
    original = loadable_table.resolve_backend
    monkeypatch.setattr(loadable_table, "resolve_backend", lambda *a, **k: calls.append(1) or original(*a, **k))
    csv_path = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv_path, index=False, sep="\t")

    _SimpleTable.load_file(sqlite_session, csv_path, dedupe=False, merge_strategy="insert")

    assert len(calls) == 1


def test_insert_merge_strategy(sqlite_session, tmp_path):
    csv_path = tmp_path / "test_table.csv"

    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    inserted = _SimpleTable.load_file(
        sqlite_session,
        csv_path,
        dedupe=False,
        merge_strategy="insert",
    )
    sqlite_session.commit()

    assert inserted == 2

    rows = sqlite_session.execute(sa.select(SimpleTable).order_by(SimpleTable.id)).scalars().all()

    assert [(r.id, r.name) for r in rows] == [
        (1, "alpha"),
        (2, "beta"),
    ]


def test_insert_adds_new_keys_to_a_populated_target_and_fails_on_an_existing_key(sqlite_session, sqlite_engine, tmp_path):
    csv_path = tmp_path / "test_table.csv"

    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv_path, index=False, sep="\t")
    _SimpleTable.load_file(sqlite_session, csv_path, dedupe=False)
    sqlite_session.commit()

    pd.DataFrame([{"id": 2, "name": "beta"}]).to_csv(csv_path, index=False, sep="\t")
    _SimpleTable.load_file(sqlite_session, csv_path, dedupe=False, merge_strategy="insert")
    sqlite_session.commit()
    rows = sqlite_session.execute(sa.select(SimpleTable.id, SimpleTable.name).order_by(SimpleTable.id)).all()
    assert [tuple(r) for r in rows] == [(1, "alpha"), (2, "beta")]

    pd.DataFrame([{"id": 2, "name": "again"}]).to_csv(csv_path, index=False, sep="\t")
    with pytest.raises(sa.exc.IntegrityError):
        _SimpleTable.load_file(sqlite_session, csv_path, dedupe=False, merge_strategy="insert")

    inspector = sa.inspect(sqlite_engine)
    staging_name = resolve_backend(sqlite_engine).staging_name_for_table(SimpleTable.__tablename__)
    assert not inspector.has_table(staging_name)


@pytest.mark.parametrize("merge_strategy", ["replace", "upsert"])
def test_empty_target_runs_the_requested_merge(sqlite_session, tmp_path, caplog, merge_strategy):
    csv_path = tmp_path / "test_table.csv"

    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    caplog.set_level(logging.INFO, logger="orm_loader.tables.loadable_table")

    inserted = _SimpleTable.load_file(
        sqlite_session,
        csv_path,
        dedupe=False,
        merge_strategy=merge_strategy,
    )
    sqlite_session.commit()

    assert inserted == 2

    rows = sqlite_session.execute(sa.select(SimpleTable).order_by(SimpleTable.id)).scalars().all()
    assert [(r.id, r.name) for r in rows] == [
        (1, "alpha"),
        (2, "beta"),
    ]

    messages = [record.getMessage() for record in caplog.records]

    if merge_strategy == "replace":
        assert any("Merge replace delete phase starting." in message for message in messages)
        assert any("Merge insert phase starting." in message for message in messages)
    else:
        assert any("Merge upsert phase starting." in message for message in messages)
        assert not any("Merge insert phase starting." in message for message in messages)


def test_staging_table_is_created_and_dropped(sqlite_session, sqlite_engine, tmp_path):
    csv_path = tmp_path / "test_table.csv"

    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv_path, index=False)

    _SimpleTable.load_file(
        sqlite_session,
        csv_path,
        dedupe=False,
    )
    sqlite_session.commit()

    inspector = sa.inspect(sqlite_engine)
    staging_name = resolve_backend(sqlite_engine).staging_name_for_table(SimpleTable.__tablename__)
    assert not inspector.has_table(staging_name)


def test_composite_pk_replace_merge(sqlite_session, tmp_path):
    csv = tmp_path / "composite_table.csv"

    pd.DataFrame(
        [
            {"a": 1, "b": 1, "value": "x"},
            {"a": 1, "b": 2, "value": "y"},
        ]
    ).to_csv(csv, index=False)

    CompositeTable.load_file(sqlite_session, csv)
    sqlite_session.commit()

    pd.DataFrame(
        [
            {"a": 1, "b": 1, "value": "x_updated"},
        ]
    ).to_csv(csv, index=False)

    CompositeTable.load_file(
        sqlite_session,
        csv,
        merge_strategy="replace",
    )
    sqlite_session.commit()

    rows = (
        sqlite_session.execute(sa.select(CompositeTable).order_by(CompositeTable.a, CompositeTable.b))
        .scalars()
        .all()
    )

    assert [(r.a, r.b, r.value) for r in rows] == [
        (1, 1, "x_updated"),
        (1, 2, "y"),
    ]


def test_filename_must_match_tablename(sqlite_session, tmp_path):
    csv = tmp_path / "wrong_name.csv"
    pd.DataFrame([{"id": 1, "name": "x"}]).to_csv(csv, index=False)

    with pytest.raises(ValueError, match="does not match table"):
        SimpleTable.load_file(
            sqlite_session,
            csv,
        )


def test_nullable_column_with_nan_does_not_crash(sqlite_session, sqlite_engine, tmp_path):
    class NullableTable(Base, FileLoadableTableInterface):
        __tablename__ = "nullable_table"
        __table_args__ = {"schema": SchemaRole.PRIMARY.value}

        id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
        flag: so.Mapped[str | None] = so.mapped_column(sa.String, nullable=True)

    Base.metadata.create_all(sqlite_engine)
    _NullableTable = cast(Type[FileTableProtocol], NullableTable)

    csv = tmp_path / "nullable_table.csv"
    pd.DataFrame(
        [
            {"id": 1, "flag": "S"},
            {"id": 2, "flag": None},  # becomes NaN in pandas
        ]
    ).to_csv(csv, index=False)

    inserted = _NullableTable.load_file(
        sqlite_session,
        csv,
        dedupe=False,
    )
    sqlite_session.commit()

    assert inserted == 2

    rows = sqlite_session.execute(sa.select(NullableTable).order_by(NullableTable.id)).scalars().all()

    assert [(r.id, r.flag) for r in rows] == [
        (1, "S"),
        (2, None),
    ]


def test_embedded_newline_in_field_is_preserved(sqlite_session, sqlite_engine, tmp_path):
    class TextTable(Base, FileLoadableTableInterface):
        __tablename__ = "text_table"
        __table_args__ = {"schema": SchemaRole.PRIMARY.value}

        id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
        name: so.Mapped[str] = so.mapped_column(sa.String)

    Base.metadata.create_all(sqlite_engine)
    _TextTable = cast(Type[FileTableProtocol], TextTable)

    csv = tmp_path / "text_table.csv"

    # Properly quoted CSV with embedded newline
    csv.write_text('id\tname\n1\t"hello\nworld"\n')

    _TextTable.load_file(
        sqlite_session,
        csv,
        dedupe=False,
        quote_mode="csv",
    )
    sqlite_session.commit()

    rows = sqlite_session.execute(sa.select(TextTable)).scalars().all()
    assert rows[0].name == "hello\nworld"


def test_embedded_tab_in_field(sqlite_session, sqlite_engine, tmp_path):
    class TextTable2(Base, FileLoadableTableInterface):
        __tablename__ = "tab_table"
        __table_args__ = {"schema": SchemaRole.PRIMARY.value}

        id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
        name: so.Mapped[str] = so.mapped_column(sa.String)

    Base.metadata.create_all(sqlite_engine)
    _TextTable2 = cast(Type[FileTableProtocol], TextTable2)

    csv = tmp_path / "tab_table.csv"
    csv.write_text('id\tname\n1\t"foo\tbar"\n')

    _TextTable2.load_file(
        sqlite_session,
        csv,
        dedupe=False,
        quote_mode="csv",
    )
    sqlite_session.commit()

    rows = sqlite_session.execute(sa.select(TextTable2)).scalars().all()
    assert rows[0].name == "foo\tbar"


# --- index_strategy tests ---


def _make_ddl_tracker(sqlite_engine):
    """Return a list that is populated with DROP/CREATE INDEX statements as they execute."""
    ddl_log: list[str] = []

    def _capture(*args):
        statement: str = args[2]
        upper = statement.strip().upper()
        if upper.startswith("DROP INDEX") or upper.startswith("CREATE INDEX"):
            ddl_log.append(statement.strip())

    sae.listen(sqlite_engine, "before_cursor_execute", _capture)
    return ddl_log


def test_auto_strategy_keeps_indices_on_sqlite(sqlite_session, sqlite_engine, tmp_path):
    """On SQLite, 'auto' resolves to 'keep' — no index DDL should be emitted."""
    ddl_log = _make_ddl_tracker(sqlite_engine)
    csv_path = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]).to_csv(
        csv_path, index=False, sep="\t"
    )

    _SimpleTable.load_file(sqlite_session, csv_path, index_strategy="auto")
    sqlite_session.commit()

    assert not any("DROP INDEX" in s.upper() for s in ddl_log)
    assert not any("CREATE INDEX" in s.upper() for s in ddl_log)
    inspector = sa.inspect(sqlite_engine)
    inspector.clear_cache()
    assert "ix_test_table_name" in {idx["name"] for idx in inspector.get_indexes("test_table")}


def test_explicit_keep_preserves_indices(sqlite_session, sqlite_engine, tmp_path):
    """Explicit 'keep' emits no index DDL regardless of dialect."""
    ddl_log = _make_ddl_tracker(sqlite_engine)
    csv_path = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv_path, index=False, sep="\t")

    _SimpleTable.load_file(sqlite_session, csv_path, index_strategy="keep")
    sqlite_session.commit()

    assert not any("DROP INDEX" in s.upper() for s in ddl_log)
    inspector = sa.inspect(sqlite_engine)
    inspector.clear_cache()
    assert "ix_test_table_name" in {idx["name"] for idx in inspector.get_indexes("test_table")}


def test_explicit_drop_rebuild_on_sqlite_restores_index(sqlite_session, sqlite_engine, tmp_path):
    """Explicit 'drop_rebuild' drops then restores the index even on SQLite."""
    ddl_log = _make_ddl_tracker(sqlite_engine)
    csv_path = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]).to_csv(
        csv_path, index=False, sep="\t"
    )

    _SimpleTable.load_file(sqlite_session, csv_path, index_strategy="drop_rebuild")
    sqlite_session.commit()

    assert any("DROP INDEX" in s.upper() for s in ddl_log)
    assert any("CREATE INDEX" in s.upper() for s in ddl_log)
    inspector = sa.inspect(sqlite_engine)
    inspector.clear_cache()
    assert "ix_test_table_name" in {idx["name"] for idx in inspector.get_indexes("test_table")}


def test_a_failed_index_rebuild_raises_after_the_merge_is_committed(sqlite_session, tmp_path, monkeypatch):
    csv_path = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv_path, index=False, sep="\t")

    def failing_create_index(index):
        raise sa.exc.OperationalError("CREATE INDEX", {}, Exception("disk full"))

    monkeypatch.setattr(sa.schema, "CreateIndex", failing_create_index)

    with pytest.raises(IngestError, match="ix_test_table_name"):
        _SimpleTable.load_file(sqlite_session, csv_path, index_strategy="drop_rebuild")

    assert sqlite_session.execute(sa.select(sa.func.count()).select_from(SimpleTable)).scalar_one() == 1


def test_a_failed_merge_is_not_masked_by_a_failed_index_rebuild(sqlite_session, tmp_path, monkeypatch):
    csv_path = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "a"}, {"id": 1, "name": "b"}]).to_csv(csv_path, index=False, sep="\t")
    monkeypatch.setattr(sa.schema, "CreateIndex", lambda index: (_ for _ in ()).throw(RuntimeError("no index")))

    with pytest.raises(sa.exc.IntegrityError):
        _SimpleTable.load_file(sqlite_session, csv_path, index_strategy="drop_rebuild")


def test_drop_rebuild_logging_shows_merge_phases(sqlite_session, tmp_path, caplog):
    csv_path = tmp_path / "test_table.csv"

    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    _SimpleTable.load_file(sqlite_session, csv_path, dedupe=False)
    sqlite_session.commit()

    pd.DataFrame(
        [
            {"id": 2, "name": "beta_updated"},
            {"id": 3, "name": "gamma"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    caplog.set_level(logging.INFO, logger="orm_loader")

    _SimpleTable.load_file(
        sqlite_session,
        csv_path,
        dedupe=False,
        merge_strategy="replace",
        index_strategy="drop_rebuild",
    )
    sqlite_session.commit()

    messages = [record.getMessage() for record in caplog.records]

    assert any("Dropping 1 active indices." in message for message in messages)
    assert any("Finished dropping 1 active indices in " in message for message in messages)
    assert any("Committing after index drop." in message for message in messages)
    assert any("Commit after index drop completed in " in message for message in messages)
    assert any("Foreign key checks disabled." in message for message in messages)
    assert any("Merging staging data into target table" in message for message in messages)
    assert any("Merge replace delete phase starting." in message for message in messages)
    assert any("Merge replace delete phase completed in " in message for message in messages)
    assert any("Merge insert phase starting." in message for message in messages)
    assert any("Merge insert phase completed in " in message for message in messages)
    assert any("Merging staging data into target table" in message for message in messages)
    assert any("Committing merged rows." in message for message in messages)
    assert any("Merge commit completed in " in message for message in messages)
    assert any("Foreign key checks restored." in message for message in messages)
    assert any("Verifying/Rebuilding indices." in message for message in messages)
    assert any("Restoring missing index: ix_test_table_name" in message for message in messages)
    assert any("Restored missing index `ix_test_table_name` in " in message for message in messages)
    assert any("Committing restored index `ix_test_table_name`." in message for message in messages)
    assert any(
        "Commit after restoring index `ix_test_table_name` completed in " in message
        for message in messages
    )
    assert any("Index verification/rebuild completed in " in message for message in messages)


def test_invalid_index_strategy_raises(sqlite_session, tmp_path):
    """An unrecognised strategy value raises ValueError before any DB work."""
    csv_path = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv_path, index=False, sep="\t")

    with pytest.raises(ValueError, match="Unknown index_strategy"):
        _SimpleTable.load_file(sqlite_session, csv_path, index_strategy="not-valid")


# from hypothesis import given, strategies as st
# from sqlalchemy.orm import declarative_base
# from pathlib import Path

# from orm_loader.loaders.data.converters import perform_cast

# @given(
#     s=st.text(
#         alphabet=st.characters(
#             blacklist_categories=["Cs"],
#             blacklist_characters=["\x00"],
#         )
#     )
# )
# def test_random_strings_roundtrip_respects_casting_contract(s, tmp_path_factory):
#     tmp_path = Path(tmp_path_factory.mktemp("hypothesis_csv"))

#     BaseLocal = declarative_base()

#     class FuzzTable(BaseLocal, FileLoadableTableInterface):
#         __tablename__ = "fuzz_table"
#         id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
#         txt: so.Mapped[str] = so.mapped_column(sa.String)

#     sqlite_engine = sa.create_engine("sqlite:///:memory:", future=True)
#     BaseLocal.metadata.create_all(sqlite_engine)

#     with Session(sqlite_engine) as sqlite_session:
#         csv = tmp_path / "fuzz_table.csv"
#         pd.DataFrame([{"id": 1, "txt": s}]).to_csv(csv, index=False, sep="\t")

#         FuzzTable.load_file(sqlite_session, csv, dedupe=False)
#         sqlite_session.commit()

#         rows = sqlite_session.execute(sa.select(FuzzTable)).scalars().all()

#         # What the loader *should* produce according to your spec
#         expected = perform_cast(s, sa.String(), on_error=None)

#         if expected is None:
#             assert rows == []
#         else:
#             # stored value may be str-canonicalised version
#             assert rows[0].txt.encode("utf-8", errors="replace") == s.encode("utf-8", errors="replace")


def test_sqlite_failed_batched_replace_keeps_original_target_rows(sqlite_session, tmp_path):
    sqlite_session.add_all([SimpleTable(id=1, name="original-1"), SimpleTable(id=2, name="original-2")])
    sqlite_session.commit()
    csv_path = tmp_path / "test_table.csv"
    pd.DataFrame(
        [
            {"id": 1, "name": "replacement-1"},
            {"id": 2, "name": "replacement-2"},
            {"id": 2, "name": "duplicate-2"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    with pytest.raises(sa.exc.IntegrityError):
        SimpleTable.load_file(
            sqlite_session,
            csv_path,
            dedupe=False,
            merge_strategy="replace",
            merge_batch_size=2,
        )
    sqlite_session.rollback()

    rows = sqlite_session.execute(sa.select(SimpleTable).order_by(SimpleTable.id)).scalars().all()
    assert [(row.id, row.name) for row in rows] == [(1, "original-1"), (2, "original-2")]


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_validate_deferred_fks_catches_an_orphan_introduced_by_load_file(duckdb_session, duckdb_db, tmp_path):
    """cycle_a_table.b_id is part of a genuine FK cycle with cycle_b_table, so
    DuckDB never got a real constraint for it (oa-configurator's
    _skip_deferred_fk_on_duckdb no-oped the ALTER TABLE ADD CONSTRAINT): a
    load introducing an orphaned reference succeeds silently unless
    validate_deferred_fks=True catches it.

    Notes
    -----
    Rollback on duckdb_session wipes out data that has already been committed.
    Rollback-after-commit on this fixture's session is therefore unreliable for 
    asserting target-row survival.
    """
    csv_path = tmp_path / "cycle_a_table.csv"
    pd.DataFrame([{"id": 1, "b_id": 999}]).to_csv(csv_path, index=False, sep="\t")

    with pytest.raises(DeferredForeignKeyViolationError, match="cycle_a_table"):
        _CycleATable.load_file(
            duckdb_session,
            csv_path,
            dedupe=False,
            validate_deferred_fks=True,
            resolved=duckdb_db.resolved,
        )

    staging_name = resolve_backend(duckdb_session, mapper=_CycleATable).staging_name_for_table(
        _CycleATable.__tablename__
    )
    assert sa.inspect(duckdb_session.get_bind()).has_table(staging_name) is False


@pytest.mark.duckdb
@pytest.mark.db_dialect
def test_load_file_rejects_replace_with_keep_index_strategy_on_duckdb_upfront(duckdb_session, tmp_path):
    """merge_strategy='replace' does an UPDATE in place on DuckDB, which
    breaks with a live index on an updated column. This combination must be
    rejected before the staging load even starts, not surface as DuckDB's
    own opaque constraint error partway through the merge."""
    csv_path = tmp_path / f"{SimpleTable.__tablename__}.csv"
    pd.DataFrame([{"id": 1, "name": "a"}]).to_csv(csv_path, index=False, sep="\t")

    with pytest.raises(NotImplementedError, match="merge_strategy='replace' with index_strategy='keep'"):
        SimpleTable.load_file(
            duckdb_session,
            csv_path,
            dedupe=False,
            merge_strategy="replace",
            index_strategy="keep",
        )
