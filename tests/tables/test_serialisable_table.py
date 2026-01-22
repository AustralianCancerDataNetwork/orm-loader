from orm_loader.tables.serialisable_table import SerialisableTableInterface
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.orm import declarative_base

from orm_loader.tables.serialisable_table import SerialisableTableInterface

Base = declarative_base()

class ExampleTable(SerialisableTableInterface, Base):
    __tablename__ = "example"

    id: so.Mapped[int]  = so.mapped_column(sa.Integer, primary_key=True)
    a: so.Mapped[int] = so.mapped_column(sa.Integer, nullable=False)
    b: so.Mapped[int | None] = so.mapped_column(sa.Integer, nullable=True)


def test_to_dict_excludes_nulls():
    obj = ExampleTable()
    obj.id=1
    obj.a=10
    obj.b=None

    out = obj.to_dict()

    assert out == {
        "id": 1,
        "a": 10,
    }
    assert "b" not in out


def test_to_dict_includes_nulls_when_requested():
    obj = ExampleTable()
    obj.id=1
    obj.a=10
    obj.b=None

    out = obj.to_dict(include_nulls=True)

    assert out["b"] is None

def test_fingerprint_is_stable():   
    obj = ExampleTable()
    obj.id=1
    obj.a=10
    obj.b=20

    fp1 = obj.fingerprint()
    fp2 = obj.fingerprint()

    assert fp1 == fp2

def test_to_dict_only_and_exclude():
    obj = ExampleTable()
    obj.id=1
    obj.a=10
    obj.b=20

    assert obj.to_dict(only={"a"}) == {"a": 10}
    assert obj.to_dict(exclude={"b"}) == {"id": 1, "a": 10}


