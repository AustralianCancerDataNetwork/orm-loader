import io
import sqlalchemy as sa
import sqlalchemy.orm as so

from orm_loader.registry.registry import ModelRegistry
from orm_loader.registry.validation_runner import ValidationRunner
from orm_loader.registry.validation_presets import always_on_validators
from orm_loader.tables.orm_table import ORMTableBase

Base = so.declarative_base()

class FakeCSVResource:
    def __init__(self, text: str):
        self._text = text

    def open(self, *_, **__):
        return io.StringIO(self._text)

def test_validation_runner_happy_path():
    class Person(ORMTableBase, Base):
        __tablename__ = "person"
        person_id = sa.Column(sa.Integer, primary_key=True, nullable=False)
        gender_concept_id = sa.Column(sa.Integer, nullable=True)

    table_csv = FakeCSVResource(
        """cdmTableName,schema,isRequired,tableDescription
Person,cdm,Yes,Person table
"""
    )

    field_csv = FakeCSVResource(
        """cdmTableName,cdmFieldName,isRequired,cdmDatatype,isPrimaryKey,isForeignKey
Person,person_id,Yes,integer,Yes,No
Person,gender_concept_id,No,integer,No,No
"""
    )

    registry = ModelRegistry(model_version="1.0", model_name="TEST")
    registry.load_table_specs(table_csv=table_csv, field_csv=field_csv)
    registry.register_model(Person)

    runner = ValidationRunner(validators=always_on_validators())
    report = runner.run(registry)

    assert report.is_valid()
    assert report.exit_code() == 0
    assert report.issues == []


def test_missing_required_column_detected():
    class Condition(ORMTableBase, Base):
        __tablename__ = "condition"
        person_id = sa.Column(sa.Integer, primary_key=True)

        # gender_concept_id missing entirely

    table_csv = FakeCSVResource(
        """cdmTableName,schema,isRequired,tableDescription
Condition,cdm,Yes,Condition table
"""
    )

    field_csv = FakeCSVResource(
        """cdmTableName,cdmFieldName,isRequired,cdmDatatype,isPrimaryKey,isForeignKey
Condition,condition_id,Yes,integer,Yes,No
Condition,condition_concept_id,Yes,integer,No,No
"""
    )

    registry = ModelRegistry(model_version="1.0")
    registry.load_table_specs(table_csv=table_csv, field_csv=field_csv)
    registry.register_model(Condition)

    runner = ValidationRunner(validators=always_on_validators())
    report = runner.run(registry)

    assert not report.is_valid()
    assert report.exit_code() == 1

    messages = {i.message for i in report.issues}
    assert "COLUMN_MISSING" in messages


def test_nullable_primary_key_detected():
    class Concept(ORMTableBase, Base):
        __tablename__ = "concept"
        person_id = sa.Column(sa.Integer, primary_key=True, nullable=True)

    table_csv = FakeCSVResource(
        """cdmTableName,schema,isRequired,tableDescription
Concept,cdm,Yes,Concept table
"""
    )

    field_csv = FakeCSVResource(
        """cdmTableName,cdmFieldName,isRequired,cdmDatatype,isPrimaryKey,isForeignKey
Concept,concept_id,Yes,integer,Yes,No
"""
    )

    registry = ModelRegistry(model_version="1.0")
    registry.load_table_specs(table_csv=table_csv, field_csv=field_csv)
    registry.register_model(Concept)

    runner = ValidationRunner(validators=always_on_validators())
    report = runner.run(registry)

    assert any(
        i.message == "PRIMARY_KEY_COLUMN_NULLABLE"
        for i in report.issues
    )


def test_foreign_key_not_in_spec_warns():
    class Parent(ORMTableBase, Base):
        __tablename__ = "parent"
        id = sa.Column(sa.Integer, primary_key=True)

    class Child(ORMTableBase, Base):
        __tablename__ = "child"
        id = sa.Column(sa.Integer, primary_key=True)
        parent_id = sa.Column(sa.Integer, sa.ForeignKey("parent.id"))

    table_csv = FakeCSVResource(
        """cdmTableName,schema,isRequired,tableDescription
Child,cdm,Yes,Child table
"""
    )

    field_csv = FakeCSVResource(

    """cdmTableName,cdmFieldName,isRequired,cdmDatatype,isPrimaryKey,isForeignKey
Child,id,Yes,integer,Yes,No
Child,parent_id,No,integer,No,No
"""
    )

    registry = ModelRegistry(model_version="1.0")
    registry.load_table_specs(table_csv=table_csv, field_csv=field_csv)
    registry.register_model(Parent)
    registry.register_model(Child)

    runner = ValidationRunner(validators=always_on_validators())
    report = runner.run(registry)

    assert any(
        i.message == "FOREIGN_KEY_NOT_IN_SPEC"
        for i in report.issues
    )

def test_fail_fast_stops_on_first_error():
    class Drug(ORMTableBase, Base):
        __tablename__ = "drug"
        drug_id = sa.Column(sa.Integer, primary_key=True, nullable=True)

    table_csv = FakeCSVResource(
        """cdmTableName,schema,isRequired,tableDescription
Drug,cdm,Yes,Drug table
"""
    )

    field_csv = FakeCSVResource(
        """cdmTableName,cdmFieldName,isRequired,cdmDatatype,isPrimaryKey,isForeignKey
Drug,drug_id,Yes,integer,Yes,No
"""
    )

    registry = ModelRegistry(model_version="1.0")
    registry.load_table_specs(table_csv=table_csv, field_csv=field_csv)
    registry.register_model(Drug)

    runner = ValidationRunner(
        validators=always_on_validators(),
        fail_fast=True,
    )

    report = runner.run(registry)

    assert len(report.issues) == 1
    assert report.issues[0].level.name == "ERROR"
