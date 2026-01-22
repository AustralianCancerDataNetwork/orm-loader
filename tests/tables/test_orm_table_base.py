from sqlalchemy.exc import NoInspectionAvailable
import sqlalchemy.orm as so
import sqlalchemy as sa
from orm_loader.tables.orm_table import ORMTableBase
import pytest

Base = so.declarative_base()

def test_pk_introspection():
    class T(ORMTableBase, Base):
        __tablename__ = "t"
        id = sa.Column(sa.Integer, primary_key=True)

    assert T.pk_names() == ["id"]

def test_pk_missing_raises():
    class T(ORMTableBase):
        __tablename__ = "t"
    with pytest.raises(NoInspectionAvailable):
        T.pk_columns()
