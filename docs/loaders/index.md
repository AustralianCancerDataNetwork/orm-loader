# Loaders

The `orm_loader.loaders` module provides **conservative, schema-aware file
ingestion infrastructure** for loading external data into ORM-backed
staging tables.

This subsystem is designed to handle:

- untrusted or messy source files
- large datasets requiring chunked processing
- incremental and repeatable loads
- dialect-specific optimisations (e.g. PostgreSQL COPY)
- explicit, inspectable failure modes

Loaders are intentionally **infrastructure-only**:
they do not embed domain rules or business semantics.

---

## Core concepts

### LoaderContext

[`LoaderContext`](context.md)

A `LoaderContext` object carries all state required to load a single file:

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

Concrete implementations differ only in **how data is read and processed**,
not in how it is staged.

---

### Staging tables

Loaders always write to **staging tables**, never directly to production
tables.

This allows:

- safe rollback
- repeatable merges
- database-level deduplication
- bulk loading optimisations

Final merge semantics are handled by the table mixins, not by loaders.

---

## Provided loaders

| Loader | Use case |
|------|----------|
| `PandasLoader` | Flexible, debuggable CSV ingestion |
| `ParquetLoader` | High-volume, columnar ingestion |

Both loaders share the same lifecycle and guarantees.

---

## Loader lifecycle

1. Detect file format and encoding  
2. Read data in chunks or batches  
3. Optionally normalise to ORM column types  
4. Optionally deduplicate (internal and/or database-level)  
5. Insert into staging table  
6. Return row count  

No implicit commits or merges occur at this layer.

---

## Guarantees

The loaders subsystem guarantees:

- deterministic ingestion behaviour
- no silent data loss
- explicit logging of dropped or malformed rows
- isolation from domain-specific rules

It does **not** guarantee correctness of the source data.

[`Helpers`](helpers.md)
