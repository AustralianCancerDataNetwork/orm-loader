"""
Tests for the explicit row-delete (_delete column) feature.

Organised by implementation phase:
  Phase 1 — foundation utilities (no DB required)
  Phase 2 — staging table schema (backend unit tests live in test_*_backend.py)
  Phase 3 — loader validation and deduplication
  Phase 4 — SQLite integration tests for merge strategies
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Type, cast

import pandas as pd
import pyarrow as pa
import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.orm import declarative_base

from orm_loader.constants import RESERVED_COLUMN_DELETE, RESERVED_COLUMN_ROWNUM
from orm_loader.loaders.loader_interface import _normalise_delete_value
from orm_loader.loaders.loading_helpers import detect_source_columns, has_delete_column
from orm_loader.tables import CSVLoadableTableInterface

# ---------------------------------------------------------------------------
# Shared test model
# ---------------------------------------------------------------------------

_Base = declarative_base()


class _DeleteTable(_Base, CSVLoadableTableInterface):
    __tablename__ = "_delete_table"
    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


class _DeleteCompositeTable(_Base, CSVLoadableTableInterface):
    __tablename__ = "_delete_composite_table"
    a: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    b: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    value: so.Mapped[str] = so.mapped_column(sa.String, nullable=True)


class _DeleteModelTable(_Base, CSVLoadableTableInterface):
    """Model that has a genuine column named '_delete' — triggers the naming collision guard."""
    __tablename__ = "_delete_model_table"
    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    _delete: so.Mapped[bool] = so.mapped_column("_delete", sa.Boolean, nullable=True)


@pytest.fixture
def del_engine():
    engine = sa.create_engine("sqlite:///:memory:", future=True)
    _Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def del_session(del_engine):
    with so.Session(del_engine) as s:
        yield s


# ---------------------------------------------------------------------------
# Phase 1 — foundation utilities
# ---------------------------------------------------------------------------


class TestConstants:
    def test_reserved_column_delete_value(self):
        assert RESERVED_COLUMN_DELETE == "_delete"

    def test_reserved_column_rownum_value(self):
        assert RESERVED_COLUMN_ROWNUM == "_rownum"


class TestDetectSourceColumns:
    def test_csv_returns_lowercased_columns(self, tmp_path: Path):
        f = tmp_path / "sample.csv"
        f.write_text("ID,Name,Value\n1,alice,10\n")
        assert detect_source_columns(f) == ["id", "name", "value"]

    def test_tsv_returns_lowercased_columns(self, tmp_path: Path):
        f = tmp_path / "sample.tsv"
        f.write_text("ID\tName\n1\talice\n")
        assert detect_source_columns(f) == ["id", "name"]

    def test_csv_with_delete_column(self, tmp_path: Path):
        f = tmp_path / "sample.csv"
        f.write_text("id,name,_delete\n1,alice,false\n")
        assert "_delete" in detect_source_columns(f)

    def test_csv_strips_hash_suffix(self, tmp_path: Path):
        f = tmp_path / "sample.csv"
        f.write_text("id_hash,name\n1,alice\n")
        assert detect_source_columns(f) == ["id", "name"]

    def test_parquet_returns_schema_names(self, tmp_path: Path):
        f = tmp_path / "sample.parquet"
        table = pa.table({"id": [1, 2], "name": ["a", "b"], "_delete": [False, True]})
        import pyarrow.parquet as pq
        pq.write_table(table, f)
        cols = detect_source_columns(f)
        assert "id" in cols
        assert "name" in cols
        assert "_delete" in cols


class TestHasDeleteColumn:
    def test_true_when_present(self):
        assert has_delete_column(["id", "name", "_delete"]) is True

    def test_false_when_absent(self):
        assert has_delete_column(["id", "name"]) is False

    def test_false_for_empty_list(self):
        assert has_delete_column([]) is False

    def test_case_sensitive_no_match_on_caps(self):
        # Column normalisation lowercases, but the function checks exact match.
        # detect_source_columns already lowercases before has_delete_column is called.
        assert has_delete_column(["_DELETE"]) is False


class TestNormaliseDeleteValue:
    @pytest.mark.parametrize("v", [True, 1, "true", "True", "TRUE", "1", "t", "T", "yes", "YES", "Yes"])
    def test_truthy_values_return_true(self, v):
        assert _normalise_delete_value(v) is True

    @pytest.mark.parametrize("v", [False, 0, "false", "False", "FALSE", "0", "f", "F", "no", "NO", "No"])
    def test_falsy_values_return_false(self, v):
        assert _normalise_delete_value(v) is False

    def test_none_returns_none(self):
        assert _normalise_delete_value(None) is None

    def test_nan_returns_none(self):
        assert _normalise_delete_value(float("nan")) is None

    def test_pd_na_returns_none(self):
        assert _normalise_delete_value(pd.NA) is None

    @pytest.mark.parametrize("v", ["banana", "yes_please", "2", 2, "delete", "tRue!"])
    def test_invalid_values_raise_value_error(self, v):
        with pytest.raises(ValueError, match="_delete"):
            _normalise_delete_value(v)

    def test_error_message_names_offending_value(self):
        with pytest.raises(ValueError, match="banana"):
            _normalise_delete_value("banana")


# ---------------------------------------------------------------------------
# Phase 3 — PandasLoader: validation and deduplication
# ---------------------------------------------------------------------------


class TestPandasLoaderDeleteValidation:
    def test_invalid_delete_value_raises_before_staging(self, del_session, tmp_path: Path):
        from orm_loader.loaders.loader_interface import PandasLoader

        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n1,alice,banana\n")

        with pytest.raises(ValueError, match="_delete"):
            _DeleteTable.load_csv(del_session, f, merge_strategy="replace")

    def test_string_true_normalised_to_bool(self, del_session, tmp_path: Path):
        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n1,alice,false\n2,bob,false\n")

        _DeleteTable.load_csv(del_session, f, merge_strategy="replace")
        rows = del_session.query(_DeleteTable).order_by(_DeleteTable.id).all()
        assert [r.id for r in rows] == [1, 2]


class TestPandasLoaderDedupeDeletePriority:
    def test_delete_wins_over_upsert_same_pk(self, del_session, tmp_path: Path):
        # Pre-load row id=1 so the delete has something to remove
        del_session.add(_DeleteTable(id=1, name="original"))
        del_session.commit()

        # File has id=2 as both upsert and delete-marked
        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n2,new,false\n2,new,true\n")

        # With dedupe=True: delete-marked row survives dedup for id=2
        # With upsert strategy: id=2 ends up absent
        _DeleteTable.load_csv(del_session, f, merge_strategy="upsert", dedupe=True)

        ids = [r.id for r in del_session.query(_DeleteTable).all()]
        assert 1 in ids       # untouched
        assert 2 not in ids   # deleted (delete won dedup)

    def test_delete_only_rows_after_dedup(self, del_session, tmp_path: Path):
        del_session.add(_DeleteTable(id=5, name="to_delete"))
        del_session.commit()

        f = tmp_path / "_delete_table.csv"
        # Two delete rows for same PK: dedup keeps one
        f.write_text("id,name,_delete\n5,,true\n5,,true\n")
        _DeleteTable.load_csv(del_session, f, merge_strategy="replace", dedupe=True)

        assert del_session.query(_DeleteTable).filter_by(id=5).first() is None


# ---------------------------------------------------------------------------
# Phase 3 — ParquetLoader: cast_to_model preserves _delete, dedup priority
# ---------------------------------------------------------------------------


class TestParquetLoaderDeleteColumn:
    def test_cast_to_model_preserves_delete_column(self):
        from orm_loader.loaders.data_classes import LoaderContext
        from orm_loader.loaders.loader_interface import ParquetLoader

        table = pa.table({
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["alice", "bob"], type=pa.string()),
            "_delete": pa.array([False, True], type=pa.bool_()),
        })

        ctx = cast(
            LoaderContext,
            type("_Ctx", (), {
                "tableclass": _DeleteTable,
                "has_delete_column": True,
            })(),
        )

        result = ParquetLoader.cast_to_model(table, ctx)
        assert "_delete" in result.schema.names
        assert result["_delete"].to_pylist() == [False, True]

    def test_cast_to_model_without_delete_column_unchanged(self):
        from orm_loader.loaders.data_classes import LoaderContext
        from orm_loader.loaders.loader_interface import ParquetLoader

        table = pa.table({
            "id": pa.array([1], type=pa.int64()),
            "name": pa.array(["alice"], type=pa.string()),
        })

        ctx = cast(
            LoaderContext,
            type("_Ctx", (), {
                "tableclass": _DeleteTable,
                "has_delete_column": False,
            })(),
        )

        result = ParquetLoader.cast_to_model(table, ctx)
        assert "_delete" not in result.schema.names

    def test_parquet_file_delete_rows_applied(self, del_session, tmp_path: Path):
        import pyarrow.parquet as pq

        del_session.add(_DeleteTable(id=1, name="to_delete"))
        del_session.add(_DeleteTable(id=2, name="keep"))
        del_session.commit()

        f = tmp_path / "_delete_table.parquet"
        pq.write_table(
            pa.table({
                "id": [1, 3],
                "name": ["", "new"],
                "_delete": [True, False],
            }),
            f,
        )

        _DeleteTable.load_csv(del_session, f, merge_strategy="replace")
        ids = sorted(r.id for r in del_session.query(_DeleteTable).all())
        assert ids == [2, 3]  # id=1 deleted, id=2 preserved, id=3 inserted


class TestParquetLoaderDedupeDeletePriority:
    def test_delete_wins_over_upsert_same_pk(self, del_session, tmp_path: Path):
        import pyarrow.parquet as pq

        del_session.add(_DeleteTable(id=1, name="original"))
        del_session.commit()

        f = tmp_path / "_delete_table.parquet"
        pq.write_table(
            pa.table({
                "id": [2, 2],
                "name": ["new", "new"],
                "_delete": [False, True],
            }),
            f,
        )

        _DeleteTable.load_csv(del_session, f, merge_strategy="upsert", dedupe=True)
        ids = [r.id for r in del_session.query(_DeleteTable).all()]
        assert 1 in ids
        assert 2 not in ids


# ---------------------------------------------------------------------------
# Phase 4 — SQLite integration tests for merge strategies
# ---------------------------------------------------------------------------


class TestSQLiteIntegrationReplace:
    def test_delete_marked_row_removed(self, del_session, tmp_path: Path):
        del_session.add_all([
            _DeleteTable(id=1, name="x"),
            _DeleteTable(id=2, name="y"),
            _DeleteTable(id=3, name="z"),
        ])
        del_session.commit()

        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n1,,true\n4,d,false\n")
        _DeleteTable.load_csv(del_session, f, merge_strategy="replace")

        ids = sorted(r.id for r in del_session.query(_DeleteTable).all())
        assert ids == [2, 3, 4]

    def test_delete_nonexistent_pk_is_noop(self, del_session, tmp_path: Path):
        del_session.add_all([_DeleteTable(id=1, name="x"), _DeleteTable(id=2, name="y")])
        del_session.commit()

        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n99,,true\n")
        _DeleteTable.load_csv(del_session, f, merge_strategy="replace")

        ids = sorted(r.id for r in del_session.query(_DeleteTable).all())
        assert ids == [1, 2]

    def test_mixed_file_replace(self, del_session, tmp_path: Path):
        del_session.add_all([
            _DeleteTable(id=1, name="x"),
            _DeleteTable(id=2, name="y"),
            _DeleteTable(id=3, name="z"),
        ])
        del_session.commit()

        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n1,a,false\n2,,true\n5,e,false\n")
        _DeleteTable.load_csv(del_session, f, merge_strategy="replace")

        rows = {r.id: r.name for r in del_session.query(_DeleteTable).all()}
        assert rows == {1: "a", 3: "z", 5: "e"}  # id=2 deleted, id=3 preserved

    def test_all_delete_rows_only(self, del_session, tmp_path: Path):
        del_session.add_all([
            _DeleteTable(id=1, name="x"),
            _DeleteTable(id=2, name="y"),
            _DeleteTable(id=3, name="z"),
        ])
        del_session.commit()

        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n1,,true\n2,,true\n")
        _DeleteTable.load_csv(del_session, f, merge_strategy="replace")

        ids = sorted(r.id for r in del_session.query(_DeleteTable).all())
        assert ids == [3]

    def test_all_falsy_delete_unchanged_behaviour(self, del_session, tmp_path: Path):
        del_session.add(_DeleteTable(id=1, name="x"))
        del_session.commit()

        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n1,a,false\n4,d,false\n")
        _DeleteTable.load_csv(del_session, f, merge_strategy="replace")

        rows = {r.id: r.name for r in del_session.query(_DeleteTable).all()}
        assert rows == {1: "a", 4: "d"}

    def test_composite_pk_delete(self, del_session, tmp_path: Path):
        del_session.add_all([
            _DeleteCompositeTable(a=1, b=1, value="x"),
            _DeleteCompositeTable(a=1, b=2, value="y"),
        ])
        del_session.commit()

        f = tmp_path / "_delete_composite_table.csv"
        f.write_text("a,b,value,_delete\n1,1,,true\n2,1,z,false\n")
        _DeleteCompositeTable.load_csv(del_session, f, merge_strategy="replace")

        rows = {(r.a, r.b): r.value for r in del_session.query(_DeleteCompositeTable).all()}
        assert rows == {(1, 2): "y", (2, 1): "z"}


class TestSQLiteIntegrationUpsert:
    def test_mixed_file_upsert(self, del_session, tmp_path: Path):
        del_session.add_all([
            _DeleteTable(id=1, name="x"),
            _DeleteTable(id=2, name="y"),
        ])
        del_session.commit()

        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n3,c,false\n1,,true\n")
        _DeleteTable.load_csv(del_session, f, merge_strategy="upsert")

        rows = {r.id: r.name for r in del_session.query(_DeleteTable).all()}
        assert rows == {2: "y", 3: "c"}  # id=1 deleted, id=2 preserved, id=3 inserted

    def test_delete_wins_over_upsert_same_pk_no_dedupe(self, del_session, tmp_path: Path):
        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n2,new,false\n2,,true\n")
        _DeleteTable.load_csv(del_session, f, merge_strategy="upsert")

        assert del_session.query(_DeleteTable).filter_by(id=2).first() is None

    def test_upsert_nonexistent_delete_is_noop(self, del_session, tmp_path: Path):
        del_session.add(_DeleteTable(id=1, name="x"))
        del_session.commit()

        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n99,,true\n")
        _DeleteTable.load_csv(del_session, f, merge_strategy="upsert")

        assert del_session.query(_DeleteTable).count() == 1


class TestSQLiteIntegrationInsertIfEmpty:
    def test_delete_marked_rows_raise_value_error(self, del_session, tmp_path: Path):
        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n1,alice,true\n")

        with pytest.raises(ValueError, match="insert_if_empty"):
            _DeleteTable.load_csv(del_session, f, merge_strategy="insert_if_empty")

        assert del_session.query(_DeleteTable).count() == 0

    def test_all_falsy_delete_insert_if_empty_succeeds(self, del_session, tmp_path: Path):
        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n1,alice,false\n")

        _DeleteTable.load_csv(del_session, f, merge_strategy="insert_if_empty")
        assert del_session.query(_DeleteTable).count() == 1


class TestSQLiteIntegrationEmptyTarget:
    def test_empty_target_delete_rows_skipped_no_error(self, del_session, tmp_path: Path):
        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n1,alice,false\n99,,true\n")

        _DeleteTable.load_csv(del_session, f, merge_strategy="replace")

        ids = [r.id for r in del_session.query(_DeleteTable).all()]
        assert ids == [1]  # only upsert row inserted

    def test_no_delete_column_behaviour_unchanged(self, del_session, tmp_path: Path):
        del_session.add(_DeleteTable(id=1, name="x"))
        del_session.commit()

        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name\n1,a\n4,d\n")
        _DeleteTable.load_csv(del_session, f, merge_strategy="replace")

        rows = {r.id: r.name for r in del_session.query(_DeleteTable).all()}
        assert rows == {1: "a", 4: "d"}


# ---------------------------------------------------------------------------
# Phase 5 — Orchestration: naming collision guard + honour_delete_marker
# ---------------------------------------------------------------------------


class TestNamingCollisionGuard:
    def test_model_with_delete_column_raises_before_staging(self, del_session, tmp_path: Path):
        f = tmp_path / "_delete_model_table.csv"
        f.write_text("id,_delete\n1,true\n")

        with pytest.raises(ValueError, match="_delete"):
            _DeleteModelTable.load_csv(del_session, f, merge_strategy="replace")

        assert del_session.query(_DeleteModelTable).count() == 0

    def test_model_with_delete_column_honour_false_bypasses_guard(self, del_session, tmp_path: Path):
        f = tmp_path / "_delete_model_table.csv"
        f.write_text("id,_delete\n1,true\n")

        _DeleteModelTable.load_csv(del_session, f, merge_strategy="replace", honour_delete_marker=False)

        assert del_session.query(_DeleteModelTable).count() == 1

    def test_no_collision_when_model_lacks_delete_column(self, del_session, tmp_path: Path):
        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n1,alice,false\n")

        _DeleteTable.load_csv(del_session, f, merge_strategy="replace")

        assert del_session.query(_DeleteTable).count() == 1


class TestHonourDeleteMarker:
    def test_false_loads_delete_rows_as_data(self, del_session, tmp_path: Path):
        del_session.add(_DeleteTable(id=1, name="original"))
        del_session.commit()

        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n1,updated,true\n")

        # With honour_delete_marker=False the _delete column is ignored;
        # replace strategy: delete id=1 then re-insert it with name="updated".
        _DeleteTable.load_csv(del_session, f, merge_strategy="replace", honour_delete_marker=False)

        row = del_session.query(_DeleteTable).filter_by(id=1).first()
        assert row is not None
        assert row.name == "updated"

    def test_true_default_applies_delete_convention(self, del_session, tmp_path: Path):
        del_session.add(_DeleteTable(id=1, name="original"))
        del_session.commit()

        f = tmp_path / "_delete_table.csv"
        f.write_text("id,name,_delete\n1,,true\n")

        _DeleteTable.load_csv(del_session, f, merge_strategy="replace")

        assert del_session.query(_DeleteTable).filter_by(id=1).first() is None
