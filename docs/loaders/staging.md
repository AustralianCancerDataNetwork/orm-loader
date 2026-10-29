# Staging and Sinks

`stage_file(ctx, reader)` connects a reader, the batch processor and the sink of the session's backend. It loads into the staging table, respects the `LoaderContext` flags, returns the number of rows left in staging, and leaves the merge to the table layer.

::: orm_loader.loaders.loader.stage_file

---

## Interpretation

- Values are interpreted by column type, as described in [Conversion](conversion.md).
- With the default policy an unreadable value or a missing required value raises and nothing is loaded.
- With `ErrorPolicy(on_error="reject_row")` those rows are written to `<rejects_dir>/<file name>.rejects.csv` and the rest load. `rejects_dir` defaults to a `rejects` directory inside the input file's directory. `LoadReport` accounts for every row read.
- With `normalise=False` raw text is passed to the staging table unchanged.

---

## Sinks

Each backend uses the fastest bulk interface its engine offers.

| Backend | Sink |
|------|------|
| PostgreSQL | One `COPY` from Arrow-written CSV |
| DuckDB | Insert from a temporary view over the Arrow batch, committed every five batches |
| SQLite | Native `executemany` per batch |

A failure in the sink is raised. There is no fallback to another loading path.

---

## Deduplication

With `dedupe` set, the staging table keeps the first row for each primary key, in source order. The backend removes the others after the load and reports a `DedupeResult` on `LoadReport` with the rows before and the rows removed. The merge step decides what happens when incoming rows overlap with existing target rows.

No schema changes are performed at the loader layer.
