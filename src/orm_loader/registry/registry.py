
from dataclasses import dataclass
from typing import Optional, Type
import sqlalchemy as sa
import csv
import importlib
import pkgutil
import logging
from ..tables.typing import ORMTableProtocol

logger = logging.getLogger(__name__)


"""
Model Registry and Specifications
=================================

This module provides infrastructure for registering ORM models and
comparing them against external table and field specifications.

It is designed to support:
- schema-aware but domain-agnostic validation
- external specifications (currently CSV-based, OMOP-style)
- structural inspection of ORM models
- downstream validation and reporting workflows

No domain rules or business logic are enforced here.
"""

@dataclass(frozen=True)
class TableSpec:

    """
    Table-level specification descriptor.

    Represents metadata about a table as defined in an external
    specification source.

    Attributes
    ----------
    table_name
        Logical name of the table.
    schema
        Schema or namespace in which the table is defined.
    is_required
        Whether the table is required by the specification.
    description
        Human-readable description of the table.
    user_guidance
        Optional additional guidance for implementers.
    """
    table_name: str
    schema: str
    is_required: bool
    description: str
    user_guidance: Optional[str] = None

@dataclass(frozen=True)
class FieldSpec:
    """
    Field-level specification descriptor.

    Represents metadata about a field/column as defined in an external
    specification source.

    Attributes
    ----------
    table_name
        Name of the table to which the field belongs.
    field_name
        Name of the field/column.
    data_type
        Declared data type in the specification.
    is_required
        Whether the field is required.
    is_primary_key
        Whether the field is part of the primary key.
    is_foreign_key
        Whether the field is a foreign key.
    fk_table
        Referenced table name, if the field is a foreign key.
    fk_field
        Referenced field name, if the field is a foreign key.
    """
    table_name: str
    field_name: str
    data_type: str
    is_required: bool
    is_primary_key: bool
    is_foreign_key: bool
    fk_table: str | None
    fk_field: str | None

@dataclass(frozen=True)
class ModelDescriptor:
    """
    Normalised, inspectable descriptor for an ORM model class.

    This descriptor is derived from SQLAlchemy inspection and captures
    the structural characteristics of a mapped ORM table.

    It is used as the primary input to validation rules.

    Attributes
    ----------
    model_class
        The ORM model class.
    table_name
        The database table name.
    columns
        Mapping of column names to SQLAlchemy Column objects.
    primary_keys
        Set of primary key column names.
    foreign_keys
        Mapping of column name to referenced (table, field).
    """
    model_class: Type[ORMTableProtocol]
    table_name: str
    columns: dict[str, sa.Column]
    primary_keys: set[str]
    foreign_keys: dict[str, tuple[str, str]]  # col -> (table, field)


    @classmethod
    def from_model(cls, model: Type[ORMTableProtocol]) -> "ModelDescriptor":
        """
        Construct a ModelDescriptor from an ORM model class.

        Parameters
        ----------
        model
            An ORM-mapped table class.

        Returns
        -------
        ModelDescriptor
            A descriptor derived from SQLAlchemy inspection.

        Raises
        ------
        TypeError
            If the provided class is not a mapped ORM model.
        """
        mapper = sa.inspect(model)
        if not mapper:
            raise TypeError(f"{model.__name__} is not mapped on this base")
        table = mapper.local_table

        fks: dict[str, tuple[str, str]] = {}
        for col in table.columns:
            for fk in col.foreign_keys:
                fks[col.name] = (
                    fk.column.table.name,
                    fk.column.name,
                )

        return cls(
            model_class=model,
            table_name=table.name,
            columns={c.name: c for c in table.columns},
            primary_keys={c.name for c in table.primary_key.columns},
            foreign_keys=fks,
        )
    
    @property
    def cls(self) -> Type[ORMTableProtocol]:
        """
        Alias for the underlying ORM model class.

        Returns
        -------
        Type[ORMTableProtocol]
            The ORM model class.
        """
        return self.model_class


def load_table_specs(csv_resource) -> dict[str, TableSpec]:
    """
    Load table specifications from a CSV resource.

    The CSV is expected to follow the OMOP table specification format.

    Parameters
    ----------
    csv_resource
        A resource object providing an ``open`` method.

    Returns
    -------
    dict[str, TableSpec]
        Mapping of table name to table specification.
    """
    out = {}
    with csv_resource.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            out[row["cdmTableName"].lower()] = TableSpec(
                table_name=row["cdmTableName"].lower(),
                schema=row["schema"],
                is_required=row["isRequired"].lower() == "yes",
                description=row["tableDescription"],
                user_guidance=row.get("userGuidance"),
            )
    return out

def load_field_specs(csv_resource) -> dict[str, dict[str, FieldSpec]]:

    """
    Load field specifications from a CSV resource.

    The CSV is expected to follow the OMOP field specification format.

    Parameters
    ----------
    csv_resource
        A resource object providing an ``open`` method.

    Returns
    -------
    dict[str, dict[str, FieldSpec]]
        Mapping of table name to mappings of field name to field specification.
    """
    out: dict[str, dict[str, FieldSpec]] = {}
    with csv_resource.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            table = row["cdmTableName"].lower()
            field = row["cdmFieldName"].lower()
            out.setdefault(table, {})[field] = FieldSpec(
                table_name=table,
                field_name=field,
                is_required=row["isRequired"].lower() == "yes",
                data_type=row["cdmDatatype"],
                is_primary_key=row["isPrimaryKey"].lower() == "yes",
                is_foreign_key=row["isForeignKey"].lower() == "yes",
                fk_table=row.get("fkCdmTableName"),
                fk_field=row.get("fkCdmFieldName"),
            )
    return out


class ModelRegistry:
    """
    Holds a registry of ORM models along with their specifications.

    The registry coordinates:
    - loading table and field specifications
    - registering ORM model classes
    - identifying missing or required tables
    - providing model descriptors for validation

    Load table and field specifications from CSV files (currently only OMOP format supported).
    TODO: support generalised specification formats via LinkML or similar.

    Register ORM model classes and compare against specifications to confirm accurate and 
    complete implementation.

    Model-specific constraints can be created to extend context-specific validation such 
    as OMOP domain constraints, value set adherence, etc.

    Validation logic itself is implemented elsewhere and consumes the
    registry as contextual input.
    """

    def __init__(self, *, model_version: str, model_name: Optional[str] = None):
        """
        Initialise a new model registry.

        Parameters
        ----------
        model_version
            Version identifier for the model set.
        model_name
            Optional human-readable name for the model set.
        """
        self.model_version: str = model_version
        self.model_name = model_name
        self._models: dict[str, ModelDescriptor] = {}
        self._table_specs: dict[str, TableSpec] = {}
        self._field_specs: dict[str, dict[str, FieldSpec]] = {}

    def load_table_specs(self, *, table_csv, field_csv) -> None:
        """
        Load table and field specifications into the registry.

        Parameters
        ----------
        table_csv
            CSV resource containing table specifications.
        field_csv
            CSV resource containing field specifications.
        """
        self._table_specs = load_table_specs(table_csv)
        self._field_specs = load_field_specs(field_csv)

    def register_model(self, model: type) -> None:
        """
        Register a single ORM model class.

        Parameters
        ----------
        model
            ORM-mapped table class.
        """
        desc = ModelDescriptor.from_model(model)
        self._models[desc.table_name] = desc

    def models(self) -> dict[str, ModelDescriptor]:
        """
        Return registered models keyed by table name.

        Returns
        -------
        dict[str, ModelDescriptor]
            Registered model descriptors.
        """
        return self._models

    def register_models(self, models: list[type]) -> None:
        """
        Register multiple ORM model classes.

        Parameters
        ----------
        models
            Iterable of ORM-mapped table classes.
        """
        for m in models:
            self.register_model(m)

    def known_tables(self) -> set[str]:
        """
        Return table names defined in the specification.

        Returns
        -------
        set[str]
            Known table names.
        """
        return set(self._table_specs.keys())

    def registered_tables(self) -> set[str]:
        """
        Return table names registered from ORM models.

        Returns
        -------
        set[str]
            Registered table names.
        """
        return set(self._models.keys())

    def missing_required_tables(self) -> set[str]:
        """
        Return required tables missing from the registered models.

        Returns
        -------
        set[str]
            Required table names that are not implemented.
        """
        return {
            t for t, spec in self._table_specs.items()
            if spec.is_required and t not in self._models
        }
    
    def discover_models(self, package: str) -> None:
        """
        Discover and register ORM models from a Python package.

        All non-abstract ORM-mapped classes found in the package and its
        submodules are registered.

        Parameters
        ----------
        package
            Dotted import path of the package to scan.
        """
        module = importlib.import_module(package)

        for _, modname, _ in pkgutil.walk_packages(
            module.__path__, module.__name__ + "."
        ):
            mod = importlib.import_module(modname)

            for obj in vars(mod).values():
                if getattr(obj, "__abstract__", False):
                    continue
                if (
                    isinstance(obj, type)
                    and hasattr(obj, "__tablename__")
                    and hasattr(obj, "__mapper__")
                ):
                    logger.debug(f"Registering model: {obj.__tablename__}")
                    self.register_model(obj)
