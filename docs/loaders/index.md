# Loaders

The `orm_loader.loaders` module provides conservative, schema-aware file
loading into ORM-backed staging tables.

This subsystem is designed to handle:

- untrusted or messy source files
- large datasets requiring chunked processing
- repeatable staged loads
- dialect-specific optimisations (e.g. PostgreSQL COPY)
- explicit, inspectable failure modes

Loaders are intentionally **infrastructure-only**:
they do not embed domain rules or business semantics.

---

## Core concepts

### LoaderContext

[`LoaderContext`](context.md)

A `LoaderContext` object carries the state required to load one file:

- target ORM table
- database session
- staging table
- file path
- operational flags (chunking, deduplication, normalisation)

This makes loader behaviour explicit, testable, and side-effect free.

---

### LoaderInterface

[`LoaderInterface`](loaders.md)

All loaders implement a common interface:

- `orm_file_load(ctx)` — orchestrates file ingestion
- `dedupe(data, ctx)` — defines deduplication semantics

Concrete implementations mainly differ in how they read and transform incoming data.

---

### Staging tables

Loaders always write to **staging tables**, never directly to production
tables.

This gives you:

- safe rollback
- repeatable merges
- bulk loading optimisations

Final merge semantics are handled by the table mixins, not by loaders.

---

## Provided loaders

| Loader | Use case |
|------|----------|
| `PandasLoader` | Flexible CSV and TSV ingestion |
| `ParquetLoader` | Columnar or batch-oriented ingestion |

Both loaders share the same lifecycle and guarantees.

---

## Loader lifecycle

1. Detect file format and encoding  
2. Read data in chunks or batches  
3. Optionally normalise to ORM column types  
4. Optionally deduplicate within the incoming data  
5. Insert into staging table  
6. Return row count  

Final merge behaviour belongs to the table mixins and backend layer, not to the loader itself.

---

## Guarantees

The loaders subsystem guarantees:

- deterministic ingestion behaviour
- no silent data loss
- explicit logging of dropped or malformed rows
- isolation from domain-specific rules

It does **not** guarantee correctness of the source data.

[`Helpers`](helpers.md)
