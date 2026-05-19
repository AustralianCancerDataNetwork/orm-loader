from __future__ import annotations

import sqlalchemy as sa

from orm_loader.helpers.discovery import get_model_by_tablename
from orm_loader.helpers.metadata import Base


def test_get_model_by_tablename_supports_nested_inheritance() -> None:
    class Child(Base):
        __abstract__ = True

    class GrandChild(Child):
        __tablename__ = "_discovery_grandchild"
        id = sa.Column(sa.Integer, primary_key=True)

    resolved = get_model_by_tablename("_discovery_grandchild")
    assert resolved is GrandChild


def test_get_model_by_tablename_returns_none_for_unknown_table() -> None:
    assert get_model_by_tablename("_not_a_real_table_name_") is None
