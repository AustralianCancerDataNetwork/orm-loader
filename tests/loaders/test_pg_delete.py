"""
Phase 6 — PostgreSQL end-to-end tests for the _delete column convention.

Coverage:
  TC-14 / FS-12  COPY fast path is not bypassed when _delete is present in the CSV
  TC-15 / FS-14  Batched merge (merge_batch_size=1) handles delete + insert phases
                 across all three staging batches
  TC-16          Retry / idempotency — running the same load twice yields the same state
                 ORM-fallback path with has_delete_column=True (both replace and upsert)
"""
from __future__ import annotations

import pytest
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.orm as so

from tests.models import DeleteTable


# ---------------------------------------------------------------------------
# shared helpers
# ---------------------------------------------------------------------------

def _rows(session: so.Session) -> list[tuple[int, str]]:
    return [
        (r.id, r.value)
        for r in session.execute(
            sa.select(DeleteTable).order_by(DeleteTable.id)
        ).scalars().all()
    ]


def _seed(session: so.Session, rows: list[tuple[int, str]]) -> None:
    for id_, value in rows:
        session.add(DeleteTable(id=id_, value=value))
    session.commit()


# ---------------------------------------------------------------------------
# TC-14 / FS-12  COPY fast-path not bypassed
# ---------------------------------------------------------------------------

@pytest.mark.postgres
def test_pg_copy_fast_path_preserved_with_delete_column(pg_session, tmp_path, monkeypatch):
    """COPY into staging must still be called when the source CSV carries a _delete column."""
    csv = tmp_path / "delete_table.csv"
    pd.DataFrame([
        {"id": 1, "value": "alpha", "_delete": "false"},
        {"id": 2, "value": "beta",  "_delete": "true"},
    ]).to_csv(csv, index=False)

    import orm_loader.backends.postgres as pg_backend
    _original = pg_backend.quick_load_pg
    called = {"copy": False}

    def _tracking(*args, **kwargs):
        called["copy"] = True
        return _original(*args, **kwargs)

    monkeypatch.setattr(pg_backend, "quick_load_pg", _tracking)

    DeleteTable.load_csv(pg_session, csv)
    pg_session.commit()

    assert called["copy"] is True, "COPY fast path should not be bypassed for _delete CSVs"
    # row 2 is delete-marked, no prior row 2 to remove — only row 1 inserted
    assert _rows(pg_session) == [(1, "alpha")]


# ---------------------------------------------------------------------------
# basic end-to-end — replace and upsert
# ---------------------------------------------------------------------------

@pytest.mark.postgres
def test_pg_replace_with_delete_column(pg_session, tmp_path):
    """replace: delete-marked rows are NOT re-inserted; non-delete rows are."""
    _seed(pg_session, [(1, "old_alpha"), (2, "old_beta"), (3, "gamma")])

    csv = tmp_path / "delete_table.csv"
    pd.DataFrame([
        {"id": 1, "value": "new_alpha", "_delete": "false"},
        {"id": 2, "value": "DELETEME",  "_delete": "true"},
    ]).to_csv(csv, index=False)

    DeleteTable.load_csv(pg_session, csv, merge_strategy="replace")
    pg_session.commit()

    # replace removes ALL staging PKs first (1 and 2), then inserts non-delete rows (1 only)
    assert _rows(pg_session) == [(1, "new_alpha"), (3, "gamma")]


@pytest.mark.postgres
def test_pg_upsert_with_delete_column(pg_session, tmp_path):
    """upsert: delete-marked rows are removed; new non-delete rows are inserted."""
    _seed(pg_session, [(1, "alpha"), (2, "beta"), (3, "gamma")])

    csv = tmp_path / "delete_table.csv"
    pd.DataFrame([
        {"id": 2, "value": "DELETEME", "_delete": "true"},
        {"id": 4, "value": "delta",    "_delete": "false"},
    ]).to_csv(csv, index=False)

    DeleteTable.load_csv(pg_session, csv, merge_strategy="upsert")
    pg_session.commit()

    assert _rows(pg_session) == [(1, "alpha"), (3, "gamma"), (4, "delta")]


# ---------------------------------------------------------------------------
# TC-15 / FS-14  batched merge (merge_batch_size=1 forces multi-iteration path)
# ---------------------------------------------------------------------------

@pytest.mark.postgres
def test_pg_replace_batched_with_delete_column(pg_session, tmp_path):
    """replace with merge_batch_size=1 runs paginated DELETE + INSERT; delete-marked rows excluded."""
    _seed(pg_session, [(1, "old_a"), (3, "old_c"), (5, "keep")])

    csv = tmp_path / "delete_table.csv"
    pd.DataFrame([
        {"id": 1, "value": "new_a",    "_delete": "false"},
        {"id": 2, "value": "new_b",    "_delete": "false"},
        {"id": 3, "value": "DELETEME", "_delete": "true"},
    ]).to_csv(csv, index=False)

    DeleteTable.load_csv(pg_session, csv, merge_strategy="replace", merge_batch_size=1)
    pg_session.commit()

    # replace: delete staging PKs 1, 2, 3 from target → removes 1 and 3
    # insert non-delete rows: 1 ("new_a") and 2 ("new_b") — row 3 filtered
    assert _rows(pg_session) == [(1, "new_a"), (2, "new_b"), (5, "keep")]


@pytest.mark.postgres
def test_pg_upsert_batched_with_delete_column(pg_session, tmp_path):
    """upsert with merge_batch_size=1 runs paginated INSERT + DELETE; both phases correct."""
    _seed(pg_session, [(1, "alpha"), (2, "beta"), (3, "gamma")])

    csv = tmp_path / "delete_table.csv"
    pd.DataFrame([
        {"id": 2, "value": "DELETEME", "_delete": "true"},
        {"id": 4, "value": "delta",    "_delete": "false"},
        {"id": 5, "value": "epsilon",  "_delete": "false"},
    ]).to_csv(csv, index=False)

    DeleteTable.load_csv(pg_session, csv, merge_strategy="upsert", merge_batch_size=1)
    pg_session.commit()

    # INSERT batches: rows 4 and 5 added (row 2 filtered by _delete IS NOT TRUE)
    # DELETE phase: row 2 removed from target
    assert _rows(pg_session) == [(1, "alpha"), (3, "gamma"), (4, "delta"), (5, "epsilon")]


# ---------------------------------------------------------------------------
# TC-16  retry / idempotency
# ---------------------------------------------------------------------------

@pytest.mark.postgres
@pytest.mark.parametrize("merge_strategy", ["replace", "upsert"])
def test_pg_delete_column_idempotent_retry(pg_session, tmp_path, merge_strategy):
    """Running the same _delete-bearing load twice produces the same final state."""
    _seed(pg_session, [(1, "alpha"), (2, "beta"), (3, "gamma")])

    csv = tmp_path / "delete_table.csv"
    pd.DataFrame([
        {"id": 2, "value": "DELETEME", "_delete": "true"},
        {"id": 4, "value": "delta",    "_delete": "false"},
    ]).to_csv(csv, index=False)

    expected = [(1, "alpha"), (3, "gamma"), (4, "delta")]

    DeleteTable.load_csv(pg_session, csv, merge_strategy=merge_strategy)
    pg_session.commit()
    assert _rows(pg_session) == expected, "first load produced unexpected result"

    DeleteTable.load_csv(pg_session, csv, merge_strategy=merge_strategy)
    pg_session.commit()
    assert _rows(pg_session) == expected, "second load (retry) must produce identical state"


# ---------------------------------------------------------------------------
# ORM fallback with has_delete_column=True
# ---------------------------------------------------------------------------

@pytest.mark.postgres
def test_pg_orm_fallback_upsert_with_delete_column(pg_session, tmp_path, monkeypatch):
    """When COPY fails, the ORM fallback must apply _delete semantics correctly (upsert)."""
    _seed(pg_session, [(1, "alpha"), (2, "beta"), (3, "gamma")])

    csv = tmp_path / "delete_table.csv"
    pd.DataFrame([
        {"id": 2, "value": "DELETEME", "_delete": "true"},
        {"id": 4, "value": "delta",    "_delete": "false"},
    ]).to_csv(csv, index=False)

    import orm_loader.backends.postgres as pg_backend
    monkeypatch.setattr(pg_backend, "quick_load_pg", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("COPY disabled")))

    DeleteTable.load_csv(pg_session, csv, merge_strategy="upsert")
    pg_session.commit()

    assert _rows(pg_session) == [(1, "alpha"), (3, "gamma"), (4, "delta")]


@pytest.mark.postgres
def test_pg_orm_fallback_replace_with_delete_column(pg_session, tmp_path, monkeypatch):
    """When COPY fails, ORM fallback + replace strategy must exclude delete-marked rows."""
    _seed(pg_session, [(1, "old_alpha"), (2, "old_beta"), (3, "gamma")])

    csv = tmp_path / "delete_table.csv"
    pd.DataFrame([
        {"id": 1, "value": "new_alpha", "_delete": "false"},
        {"id": 2, "value": "DELETEME",  "_delete": "true"},
    ]).to_csv(csv, index=False)

    import orm_loader.backends.postgres as pg_backend
    monkeypatch.setattr(pg_backend, "quick_load_pg", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("COPY disabled")))

    DeleteTable.load_csv(pg_session, csv, merge_strategy="replace")
    pg_session.commit()

    assert _rows(pg_session) == [(1, "new_alpha"), (3, "gamma")]
