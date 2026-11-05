"""Referential integrity after a load: the same check on every dialect."""

from pathlib import Path
from typing import Type, cast

import pytest

from oa_configurator import Dialect
from orm_loader.backends import STAGING_SCHEMA, ForeignKeyViolationError
from orm_loader.tables.typing import FileTableProtocol
from tests.models import (
    ReferencedParentTable,
    ReferencingChildTable,
    SelfReferentialTable,
    SimpleTable,
    UnreferencedGrandparentTable,
)

_Child = cast(Type[FileTableProtocol], ReferencingChildTable)
_Parent = cast(Type[FileTableProtocol], ReferencedParentTable)
_Grandparent = cast(Type[FileTableProtocol], UnreferencedGrandparentTable)
_Self = cast(Type[FileTableProtocol], SelfReferentialTable)
_Simple = cast(Type[FileTableProtocol], SimpleTable)


@pytest.fixture
def options(dialect_session, request):
    """Keyword arguments every load and validation on this dialect needs."""
    if dialect_session.get_bind().dialect.name == "sqlite":
        return {"staging_schema_tag": None}
    options = {"staging_schema_tag": STAGING_SCHEMA}
    if dialect_session.get_bind().dialect.name == Dialect.DUCKDB:
        options["resolved"] = request.getfixturevalue("duckdb_db").resolved
    return options


def _load(session, table, tmp_path: Path, text: str, options: dict, **extra) -> None:
    path = tmp_path / f"{table.__tablename__}.csv"
    path.write_text(text)
    table.load_file(session, path, dedupe=False, **options, **extra)
    session.commit()


def _validate(session, table, options: dict) -> None:
    table.validate_foreign_keys(session, **options)


def test_every_reference_resolving_passes(dialect_session, tmp_path, options):
    _load(dialect_session, _Grandparent, tmp_path, "id,name\n1,g\n", options)
    _load(dialect_session, _Parent, tmp_path, "id,grandparent_id,name\n1,1,p\n", options)
    _load(dialect_session, _Child, tmp_path, "id,parent_id\n1,1\n2,1\n", options)
    for table in (_Grandparent, _Parent, _Child):
        _validate(dialect_session, table, options)


def test_a_table_without_foreign_keys_passes(dialect_session, tmp_path, options):
    _load(dialect_session, _Simple, tmp_path, "id,name\n1,a\n", options)
    _validate(dialect_session, _Simple, options)


def test_null_references_are_not_violations(dialect_session, tmp_path, options):
    _load(dialect_session, _Self, tmp_path, "id,parent_id\n1,\n2,1\n", options)
    _validate(dialect_session, _Self, options)


def test_an_orphan_is_reported_with_its_key(dialect_session, tmp_path, options):
    _load(dialect_session, _Self, tmp_path, "id,parent_id\n1,\n2,99\n", options, validate_deferred_fks=False)
    with pytest.raises(ForeignKeyViolationError, match=r"self_referential_table.*\[\(99,\)\]"):
        _validate(dialect_session, _Self, options)


def test_tables_loaded_child_first_validate_once_the_parents_are_loaded(dialect_session, tmp_path, options):
    if dialect_session.get_bind().dialect.name == Dialect.DUCKDB:
        pytest.skip("DuckDB enforces this key itself, so a child cannot be loaded first")
    _load(dialect_session, _Child, tmp_path, "id,parent_id\n1,1\n", options)
    with pytest.raises(ForeignKeyViolationError, match="referencing_child_table"):
        _validate(dialect_session, _Child, options)
    _load(dialect_session, _Parent, tmp_path, "id,grandparent_id,name\n1,1,p\n", options)
    _load(dialect_session, _Grandparent, tmp_path, "id,name\n1,g\n", options)
    for table in (_Grandparent, _Parent, _Child):
        _validate(dialect_session, table, options)
