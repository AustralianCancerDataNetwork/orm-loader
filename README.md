## orm-loader

[![Tests](https://github.com/AustralianCancerDataNetwork/orm-loader/actions/workflows/tests.yml/badge.svg)](
https://github.com/AustralianCancerDataNetwork/orm-loader/actions/workflows/tests.yml
)

A lightweight foundation for building and validating SQLAlchemy-based data models.

`orm-loader` sits below any particular schema or CDM. It gives you a small set of reusable pieces for defining tables, loading files through staging tables, and checking models against external specifications. It stays out of domain logic on purpose.

The library focuses on:

* ORM table mixins and introspection
* staged file loading
* loader and validation infrastructure
* operational helpers that work across supported backends

At the moment, the built-in backends are SQLite and PostgreSQL.


### What this library provides

The package is deliberately small. Most downstream projects only need a couple of these pieces.

1. A minimal ORM table base

`ORMTableBase` provides structural utilities for mapped tables without pulling domain rules into the base layer.

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
You can inherit from it directly or pick it up through one of the higher-level mixins.

2. CSV-based ingestion mixins

`CSVLoadableTableInterface` adds staged file loading to ORM tables. It can use pandas or PyArrow loaders, and on PostgreSQL it can use a fast `COPY` path when the input is clean enough.

Features include:
* staging table creation and cleanup
* chunked loading for large files
* optional casting and deduplication before insert
* backend-specific merge behaviour
* PostgreSQL fast-path loading with ORM fallback
* backend-aware index handling during merge

```python
class MyTable(CSVLoadableTableInterface, ORMTableBase, Base):
    __tablename__ = "my_table"

```

The main extension points here are loader choice, column mapping, and the normal SQLAlchemy model definitions themselves. Most downstream projects do not need to override much beyond `csv_columns()` and the model schema.

Two details matter in practice:

* On SQLite and on ORM-based loads, `normalise`, `dedupe`, and `chunksize` are applied by the selected loader.
* On PostgreSQL, the fast `COPY` path is deliberately lower-level. It prioritises throughput and does not apply loader-level casting, deduplication, or chunked row processing. If `COPY` fails, ingestion falls back to the ORM loader.

Merge behaviour is also backend-aware. PostgreSQL can batch very large merges using `merge_batch_size` to keep transaction size under control. That is useful for large staged loads, but once batching is triggered the merge is committed in chunks rather than as one all-or-nothing transaction.

3. Structured serialisation and hashing

`SerialisableTableInterface` adds lightweight serialisation helpers for ORM rows.

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

The library includes validation infrastructure for comparing ORM models against external specifications.

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

The library provides lightweight helpers for schema creation and bootstrapping. It does not try to replace migrations.

```python
from orm_loader.metadata import Base
from orm_loader.bootstrap import bootstrap

bootstrap(engine, create=True)
```

6. Bulk-loading helpers

There are a few lower-level helpers for trusted bulk workflows, including backend-aware foreign key management and SQLite connection setup for heavy local loads.

## Testing and coverage

Install the development extras:

```bash
pip install -e ".[dev,postgres]"
```

Run the test suite:

```bash
PYTHONPATH=src pytest
```

Run the test suite with coverage reporting:

```bash
PYTHONPATH=src pytest --cov=orm_loader --cov-branch --cov-report=term-missing --cov-report=xml
```

The coverage configuration lives in `pyproject.toml`.

Useful outputs:

* terminal summary with missing lines
* `coverage.xml` for CI tooling
* `htmlcov/` if you also run `--cov-report=html`

## Summary

This library is meant to be the boring layer underneath downstream models:

* reusable ORM mixins
* staged ingestion patterns
* validation scaffolding
* operational helpers

Domain rules, business logic, and schema semantics stay in the downstream project.

This makes it suitable as a shared foundation for:
* clinical data models
* research data marts
* registry schemas
* synthetic data pipelines
