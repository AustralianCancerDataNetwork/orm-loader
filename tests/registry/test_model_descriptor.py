from typing import cast

import sqlalchemy as sa
import sqlalchemy.orm as so
import pytest
from sqlalchemy.exc import NoInspectionAvailable
from orm_loader.registry.registry import ModelDescriptor
from orm_loader.tables.orm_table import ORMTableBase
from orm_loader.tables.typing import ORMTableProtocol

Base = so.declarative_base()


def test_model_descriptor_from_model_basic():
    class T(ORMTableBase, Base):
        __tablename__ = "t"
        id = sa.Column(sa.Integer, primary_key=True)
        a = sa.Column(sa.String)

    desc = ModelDescriptor.from_model(cast(type[ORMTableProtocol], T))

    assert desc.model_class is T
    assert desc.table_name == "t"
    assert set(desc.columns.keys()) == {"id", "a"}
    assert desc.primary_keys == {"id"}
    assert desc.foreign_keys == {}


def test_model_descriptor_detects_foreign_keys():
    class Parent(ORMTableBase, Base):
        __tablename__ = "parent"
        id = sa.Column(sa.Integer, primary_key=True)

    class Child(ORMTableBase, Base):
        __tablename__ = "child"
        id = sa.Column(sa.Integer, primary_key=True)
        parent_id = sa.Column(sa.Integer, sa.ForeignKey("parent.id"))

    desc = ModelDescriptor.from_model(cast(type[ORMTableProtocol], Child))

    assert desc.foreign_keys == {
        "parent_id": ("parent", "id")
    }


def test_model_descriptor_rejects_unmapped_class():
    class NotMapped:
        pass

    with pytest.raises(NoInspectionAvailable):
        ModelDescriptor.from_model(NotMapped) # type: ignore
