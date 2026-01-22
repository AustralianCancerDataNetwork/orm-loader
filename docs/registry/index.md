# Model Registry & Validation

The `orm_loader.registry` module provides **model-agnostic validation
infrastructure** for SQLAlchemy ORM models.

It enables:
- loading external table / field specifications
- registering ORM models
- structural comparison between models and specs
- reusable validation rules
- structured validation reports

This layer is **schema-aware but domain-agnostic**.


---

## Registry

- [`ModelRegistry`](registry.md)
- [`ValidationReport`](validation_report.md)
- [`ValidationRunner`](validation_runner.md)

---

## Model Definitions

- [`TableSpec`](descriptors.md)
- [`FieldSpec`](descriptors.md)
- [`ModelDescriptor`](descriptors.md)

---

## End-to-End Validation Example

This example demonstrates the complete validation workflow:

1. Load external specifications  
2. Register ORM models  
3. Run validation  
4. Inspect and consume validation results  


---

## 1. Inputs

### External specifications

Validation begins with **external table and field specifications**
(e.g. OMOP CSVs):

- `tables.csv` — table-level definitions
- `fields.csv` — field-level definitions

These describe *what should exist*, not how it is implemented.


### ORM models

Assume you have SQLAlchemy ORM models defined elsewhere:

```python
from sqlalchemy.orm import declarative_base
from orm_loader.tables import ORMTableBase

Base = declarative_base()

class Person(ORMTableBase, Base):
    __tablename__ = "person"
    # columns omitted for brevity

class VisitOccurrence(ORMTableBase, Base):
    __tablename__ = "visit_occurrence"
    # columns omitted for brevity

```

## 2. Create a ModelRegistry

The registry is the coordination object that holds models and specs.

```python

from orm_loader.registry import ModelRegistry

registry = ModelRegistry(
    model_version="5.4",
    model_name="OMOP"
)

```

## 3. Load Specifications

Specifications are loaded from CSV resources:

```python
from importlib.resources import files

specs = files("my_specs_package")

registry.load_table_specs(
    table_csv=specs / "tables.csv",
    field_csv=specs / "fields.csv",
)
```
At this point, the registry knows:

* which tables are required
* which fields belong to each table
* primary and foreign key expectations

## 4. Register ORM models

Models can be registered explicitly:

```python
registry.register_models([
    Person,
    VisitOccurrence,
])
```

Or discovered automatically from a package:

```python
registry.discover_models("my_project.models")
```

Each model is introspected into a `ModelDescriptor`.

## 5. Inspect Registry State

Before running validation, the registry can be queried:

```python
registry.known_tables()
registry.registered_tables()
registry.missing_required_tables()
```
This supports pre-flight checks and early failure in CI pipelines.

## 6. Define Validators

Validators are small, composable rule objects:

```python
from orm_loader.registry.validation import (
    ColumnPresenceValidator,
    ColumnNullabilityValidator,
    PrimaryKeyValidator,
    ForeignKeyShapeValidator,
)

validators = [
    ColumnPresenceValidator(),
    ColumnNullabilityValidator(),
    PrimaryKeyValidator(),
    ForeignKeyShapeValidator(),
]
```

Each validator:

* operates on a single model
* produces structured validation issues
* is independent of execution order

## 7. Run Validation
Validation is coordinated by the `ValidationRunner`:

```python
from orm_loader.registry.validation_runner import ValidationRunner

runner = ValidationRunner(
    validators=validators,
    fail_fast=False,
)

report = runner.run(registry)
```

## 8. Consume the ValidationReport

### Overall Validity

```python
report.is_valid()
```

### Top Level Summary

```python
print(report.summary())
```

### Human-readable report

```python
print(report.render_text_report())
```

### Machine-readable output (CI/CD)

```python
report.to_dict()
```