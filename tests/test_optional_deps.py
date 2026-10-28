"""Tests that orm_loader stays importable without optional extras installed
"""

from __future__ import annotations

import subprocess
import sys


def _run_blocked(import_statement: str, *, blocked_module: str = "duckdb_sqlalchemy") -> subprocess.CompletedProcess[str]:
    code = f"import sys; sys.modules['{blocked_module}'] = None; {import_statement}"
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)


def test_backends_importable_without_duckdb_sqlalchemy():
    """orm_loader.backends imports cleanly when duckdb-sqlalchemy is not installed."""
    result = _run_blocked("import orm_loader.backends")
    assert result.returncode == 0, result.stderr


def test_duckdb_backend_module_importable_without_duckdb_sqlalchemy():
    """orm_loader.backends.duckdb itself is importable without the extra."""
    result = _run_blocked("import orm_loader.backends.duckdb")
    assert result.returncode == 0, result.stderr


def test_backends_importable_without_psycopg():
    """orm_loader.backends imports cleanly when psycopg is not installed."""
    result = _run_blocked("import orm_loader.backends", blocked_module="psycopg")
    assert result.returncode == 0, result.stderr


def test_postgres_backend_module_importable_without_psycopg():
    """orm_loader.backends.postgres itself is importable without the driver."""
    result = _run_blocked("import orm_loader.backends.postgres", blocked_module="psycopg")
    assert result.returncode == 0, result.stderr
