# Staging and Sinks

`stage_file(ctx, reader, staging)` connects a reader, the batch processor and the sink of the session's backend. It loads into the given staging table, respects the `LoaderContext` flags, returns the number of rows left in staging, and leaves the merge to the table layer.

::: orm_loader.loaders.loader.stage_file

---

## Interpretation

- Values are interpreted by column type, as described in [Conversion](conversion.md).
- With the default policy an unreadable value or a missing required value raises and nothing is loaded.
- With `ErrorPolicy(on_error="reject_row")` those rows, and delimited lines with the wrong number of fields, are written to `<rejects_dir>/<file name>.rejects.csv` and the rest load. A malformed line is listed with its row number in the file, and its text is in the reason. `rejects_dir` defaults to a `rejects` directory inside the input file's directory, and the file of an earlier load is removed. `LoadReport` accounts for every row read, checked against the rows counted in the staging table.
- A blank value, and a column the file lacks, take the column's default when it has one: a server default written as text, a scalar Python default, or a SQL expression such as `current_date`. The column's handler reads the default like a value from the file, and an expression is evaluated by the database for each batch that needs it. A column with a callable default stays required.
- With `normalise=False` raw text is passed to the staging table unchanged.

---

## Staging table

Staging mirrors the target: its columns, their types and their nullability, with no key, index, default or constraint, and a `_rownum` column that orders the rows (SQLite uses its rowid). The processor completes every value before it is written, so a NOT NULL column never receives NULL. A key the file provides is a staging column like any other. When the file lacks an autoincrement key, staging leaves it out and the merge leaves it out of its insert, so the database assigns it.

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

---

## Referential integrity

On PostgreSQL and SQLite a load relaxes foreign keys while it merges, so the caller needs no foreign key handling and a row loaded before the row it references is accepted. To verify the result, call `Table.validate_foreign_keys(session)` after every table is loaded. It checks every foreign key of the table with an anti-join and raises `ForeignKeyViolationError` with up to five unmatched keys. Null references are not violations, and the load order does not matter.

To insert ORM objects built in Python that reference each other, wrap the inserts in `backend.relaxed_foreign_keys(session)`, which relaxes the keys on PostgreSQL and SQLite and leaves them enforced on DuckDB.

DuckDB enforces the keys it creates, so load parent tables first. `load_file` already validates the keys DuckDB never enforces (cross-schema, cyclic and self-referential) after each table's merge, and `validate_foreign_keys` repeats that check, so it works the same way on every backend. A DuckDB table with foreign keys needs `resolved=` for both.

