# Loader Context and Report

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
| `chunksize` | Rows per batch for the default reader of formats that batch by rows |
| `normalise` | Whether to interpret values under the policy; when false, values are written as read |
| `dedupe` | Whether the staging table keeps only the first row for each primary key |
| `quote_mode` | Quoting mode for delimited text: `csv`, `literal` or `by_delimiter` (default) |
| `policy` | `ConversionPolicy`: how values are interpreted and what happens to unreadable ones |
| `rejects_dir` | Directory that rejected rows are written to |
| `report` | `LoadReport` row accounting |

::: orm_loader.loaders.loader.LoaderContext

---

## LoadReport

`LoadReport` accounts for every row of a load: rows read, loaded, rejected and removed as duplicates. `DedupeResult` holds the key columns and the rows removed. A load that cannot account for every row raises.

::: orm_loader.loaders.loader.LoadReport
::: orm_loader.backends.base.DedupeResult

---

## Design notes

- Unreadable values and missing required values raise by default, so no row disappears without a trace
- With `ErrorPolicy(on_error="reject_row")` each rejected row is written to a rejects file with its raw text and the reason
