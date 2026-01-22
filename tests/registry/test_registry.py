from pathlib import Path
import io
from orm_loader.registry.registry import ModelRegistry
import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.tables.orm_table import ORMTableBase

from orm_loader.registry.registry import (
    load_table_specs,
    load_field_specs,
    TableSpec,
    FieldSpec,
)

Base = so.declarative_base()

class FakeResource:
    """Minimal stand-in for importlib.resources API."""
    def __init__(self, text: str):
        self._text = text

    def open(self, *_, **__):
        return io.StringIO(self._text)


def test_load_table_specs():
    csv_text = """cdmTableName,schema,isRequired,tableDescription,userGuidance
Person,cdm,Yes,Person table,Use carefully
Visit,cdm,No,Visit table,
"""
    res = FakeResource(csv_text)

    specs = load_table_specs(res)

    assert isinstance(specs["person"], TableSpec)
    assert specs["person"].is_required is True
    assert specs["visit"].is_required is False
    assert specs["person"].description == "Person table"


def test_load_field_specs():
    csv_text = """cdmTableName,cdmFieldName,isRequired,cdmDatatype,isPrimaryKey,isForeignKey,fkCdmTableName,fkCdmFieldName
Person,person_id,Yes,integer,Yes,No,,
Person,gender_concept_id,No,integer,No,Yes,Concept,concept_id
"""
    res = FakeResource(csv_text)

    specs = load_field_specs(res)

    person_fields = specs["person"]

    assert isinstance(person_fields["person_id"], FieldSpec)
    assert person_fields["person_id"].is_primary_key is True
    assert person_fields["gender_concept_id"].is_foreign_key is True
    assert person_fields["gender_concept_id"].fk_table == "Concept"



def test_register_single_model():
    class B(ORMTableBase, Base):
        __tablename__ = "b"
        id = sa.Column(sa.Integer, primary_key=True)

    reg = ModelRegistry(model_version="1.0")
    reg.register_model(B)

    models = reg.models()

    assert "b" in models
    assert models["b"].model_class is B


def test_register_multiple_models():
    class A(ORMTableBase, Base):
        __tablename__ = "a"
        id = sa.Column(sa.Integer, primary_key=True)

    class V(ORMTableBase, Base):
        __tablename__ = "v"
        id = sa.Column(sa.Integer, primary_key=True)

    reg = ModelRegistry(model_version="1.0")
    reg.register_models([A, V])

    assert reg.registered_tables() == {"a", "v"}


def test_missing_required_tables():
    reg = ModelRegistry(model_version="1.0")

    # Fake specs injected directly (unit-level test)
    reg._table_specs = { # type: ignore
        "person": type("S", (), {"is_required": True})(),
        "visit": type("S", (), {"is_required": False})(),
    }

    assert reg.missing_required_tables() == {"person"}


def test_known_vs_registered_tables():
    class R(ORMTableBase, Base):
        __tablename__ = "r"
        id = sa.Column(sa.Integer, primary_key=True)

    reg = ModelRegistry(model_version="1.0")
    reg._table_specs = { # type: ignore
        "r": type("S", (), {"is_required": True})(),
        "x": type("S", (), {"is_required": True})(),
    }

    reg.register_model(R)
    assert reg.known_tables() == {"r", "x"}
    assert reg.registered_tables() == {"r"}