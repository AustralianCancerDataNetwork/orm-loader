## orm-loader

[![Tests](https://github.com/AustralianCancerDataNetwork/orm-loader/actions/workflows/tests.yml/badge.svg)](
https://github.com/AustralianCancerDataNetwork/orm-loader/actions/workflows/tests.yml
)

A lightweight, reusable foundation for building and validating SQLAlchemy-based clinical (and non-clinical) data models.

This library provides general-purpose ORM infrastructure that sits below any specific data model (OMOP, PCORnet, custom CDMs, etc.), focusing on:

* declarative base configuration
* bulk ingestion patterns
* file-based validation & loading
* table introspection
* model-agnostic validation scaffolding
* safe, database-portable operational helpers

It intentionally contains no domain logic and no assumptions about a specific schema.


### What this library provides:

This library provides a small set of composable building blocks for defining, loading, inspecting, and validating SQLAlchemy-based data models.
All components are model-agnostic and can be selectively combined in downstream libraries.

1. A minimal, opinionated ORM table base

ORMTableBase provides structural introspection utilities for SQLAlchemy-mapped tables, without imposing any domain semantics.

It supports:
* mapper access and inspection
* primary key discovery
* required (non-nullable) column detection
* consistent primary key handling across models
* simple ID allocation helpers for sequence-less databases

```python
from orm_loader.tables import ORMTableBase

class MyTable(ORMTableBase, Base):
    __tablename__ = "my_table"

```
This base is intended to be inherited by all ORM tables, either directly or via higher-level mixins.

2. CSV-based ingestion mixins

CSVLoadableTableInterface adds opt-in CSV loading support for ORM tables using pandas, with a focus on correctness and scalability.

Features include:
* chunked loading for large files
* optional per-table normalisation logic
* optional deduplication against existing database rows
* safe bulk inserts using SQLAlchemy sessions

```python
class MyTable(CSVLoadableTableInterface, ORMTableBase, Base):
    __tablename__ = "my_table"

```

Downstream models may override:
* normalise_dataframe(...)
* dedupe_dataframe(...)
* csv_columns()
to implement table-specific ingestion policies.

3. Structured serialisation and hashing

SerialisableTableInterface adds lightweight, explicit serialisation helpers for ORM rows.

It supports:
* conversion to dictionaries
* JSON serialisation
* stable row-level fingerprints
* iterator-style access to field/value pairs

```python
row = session.get(MyTable, 1)
row.to_dict()
row.to_json()
row.fingerprint()
```

This is useful for:

* debugging
* auditing
* reproducibility checks
* downstream APIs or exports


4. Model registry and validation scaffolding

The library includes model-agnostic validation infrastructure, designed to compare ORM models against external specifications.

This includes:
* a model registry
* table and field descriptors
* validator contracts
* a validation runner
* structured validation reports
Specifications can be loaded from CSV today, with support for other formats (e.g. LinkML) planned.

```python
registry = ModelRegistry(model_version="1.0")
registry.load_table_specs(table_csv, field_csv)
registry.register_models([MyTable])

runner = ValidationRunner(validators=always_on_validators())
report = runner.run(registry)
```

Validation output is available as:
* human-readable text
* structured dictionaries
* JSON (for CI/CD integration)
* exit codes suitable for pipelines

5. Database bootstrap helpers
The library provides lightweight helpers for schema creation and bootstrapping, without imposing a migration strategy.

```python
from orm_loader.metadata import Base
from orm_loader.bootstrap import bootstrap

bootstrap(engine, create=True)
```

6. Safe bulk-loading utilities

A reusable context manager simplifies trusted bulk ingestion workflows:
* temporarily disables foreign key checks where supported
* suppresses autoflush for performance
* ensures reliable rollback on failure

## Summary

This library intentionally focuses on infrastructure, not semantics.

It provides:
* reusable ORM mixins
* safe ingestion patterns
* validation scaffolding
* database-portable utilities

while leaving domain rules, business logic, and schema semantics to downstream libraries.

This makes it suitable as a shared foundation for:
* clinical data models
* research data marts
* registry schemas
* synthetic data pipelines
