# Loader Context and Diagnostics

This page documents the shared data structures used by the loaders
subsystem.

---

## LoaderContext


`LoaderContext` is an immutable coordination object passed through
all loader operations.

It encapsulates **all state required** to load a file without relying
on globals or implicit configuration.

### Fields

| Field | Description |
|-----|-------------|
| `tableclass` | Target ORM table class |
| `session` | Active SQLAlchemy session |
| `path` | Path to the input file |
| `staging_table` | SQLAlchemy Table used for staging |
| `chunksize` | Optional chunk size |
| `normalise` | Whether to cast values to ORM types |
| `dedupe` | Whether to deduplicate incoming data |

::: orm_loader.loaders.data_classes.LoaderContext

---

## Casting statistics

Loaders track **casting failures** during normalisation to support
debugging and auditability.

### ColumnCastingStats

Tracks casting failures for a single column:

- total failure count
- representative example values

### TableCastingStats

Aggregates column-level statistics for a table.

This enables loaders to emit warnings such as:

- unexpected nulls in required columns
- values that could not be cast to target types

Statistics are logged, not raised as exceptions.

::: orm_loader.loaders.data_classes.ColumnCastingStats
::: orm_loader.loaders.data_classes.TableCastingStats

---

## Design notes

- Casting failures do **not** abort loads
- Rows violating required constraints are dropped explicitly
- Examples are capped to avoid log flooding

This design favours **observability over strict enforcement**.
