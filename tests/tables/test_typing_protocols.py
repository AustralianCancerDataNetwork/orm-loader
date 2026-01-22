import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.tables.orm_table import ORMTableBase
from orm_loader.tables.serialisable_table import SerialisableTableInterface
from orm_loader.tables.loadable_table import CSVLoadableTableInterface
from orm_loader.tables.typing import (
    ORMTableProtocol,
    CSVTableProtocol,
    SerializedTableProtocol,
)

Base = so.declarative_base()

def test_orm_table_protocol_conformance():
    class Q(ORMTableBase, Base):
        __tablename__ = "f"
        id = sa.Column(sa.Integer, primary_key=True)

    assert isinstance(Q, ORMTableProtocol)

    assert hasattr(Q, "__tablename__")
    assert callable(Q.pk_names)
    assert callable(Q.model_columns)

def test_serialized_table_protocol_conformance():
    class G(SerialisableTableInterface, ORMTableBase, Base):
        __tablename__ = "g"
        id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
        a: so.Mapped[int] = so.mapped_column(sa.Integer)

    obj = G()
    obj.a = 1

    assert isinstance(obj, SerializedTableProtocol)

    # Protocol methods exist
    assert callable(obj.to_dict)
    assert callable(obj.to_json)
    assert callable(obj.fingerprint)

def test_csv_table_protocol_conformance():
    class P(CSVLoadableTableInterface, ORMTableBase, Base):
        __tablename__ = "p"
        id = sa.Column(sa.Integer, primary_key=True)

    assert isinstance(P, CSVTableProtocol)  
    # Required class-level API
    assert callable(P.load_csv)
    assert callable(P.create_staging_table)
    assert callable(P.merge_from_staging)