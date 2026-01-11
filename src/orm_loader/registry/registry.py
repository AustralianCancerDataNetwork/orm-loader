
from dataclasses import dataclass
from typing import Optional, Iterable, Type
import sqlalchemy as sa
import csv, importlib, pkgutil
import logging
from ..tables.base.typing import ORMTableProtocol
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class TableSpec:
    table_name: str
    schema: str
    is_required: bool
    description: str
    user_guidance: Optional[str] = None

@dataclass(frozen=True)
class FieldSpec:
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
    model_class: Type[ORMTableProtocol]
    table_name: str
    columns: dict[str, sa.Column]
    primary_keys: set[str]
    foreign_keys: dict[str, tuple[str, str]]  # col -> (table, field)


    @classmethod
    def from_model(cls, model: Type[ORMTableProtocol]) -> "ModelDescriptor":
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
        return self.model_class


def load_table_specs(csv_resource) -> dict[str, TableSpec]:
    """Currently OMOP specifications only."""
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
    """Currently OMOP specifications only."""
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

    Load table and field specifications from CSV files (currently only OMOP format supported).
    TODO: support generalised specification formats via LinkML or similar.

    Register ORM model classes and compare against specifications to confirm accurate and 
    complete implementation.

    Model-specific constraints can be created to extend context-specific validation such 
    as OMOP domain constraints, value set adherence, etc.
    """

    def __init__(self, *, model_version: str, model_name: Optional[str] = None):
        self.model_version: str = model_version
        self.model_name = model_name
        self._models: dict[str, ModelDescriptor] = {}
        self._table_specs: dict[str, TableSpec] = {}
        self._field_specs: dict[str, dict[str, FieldSpec]] = {}

    def load_table_specs(self, *, table_csv, field_csv) -> None:
        self._table_specs = load_table_specs(table_csv)
        self._field_specs = load_field_specs(field_csv)

    def register_model(self, model: type) -> None:
        desc = ModelDescriptor.from_model(model)
        self._models[desc.table_name] = desc

    def models(self) -> dict[str, ModelDescriptor]:
        return self._models

    def register_models(self, models: list[type]) -> None:
        for m in models:
            self.register_model(m)

    def known_tables(self) -> set[str]:
        return set(self._table_specs.keys())

    def registered_tables(self) -> set[str]:
        return set(self._models.keys())

    def missing_required_tables(self) -> set[str]:
        return {
            t for t, spec in self._table_specs.items()
            if spec.is_required and t not in self._models
        }
    
    def discover_models(self, package: str) -> None:
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
