# Loader Helper Utilities

This page covers the low-level functions that support the loader implementations.

---

## Encoding and delimiter detection

### `infer_encoding(path)`

Detects file encoding using `chardet`.

- ASCII is normalised to UTF-8
- Only a small prefix of the file is inspected

### `infer_delim(path)`

Detects CSV delimiter by inspecting the first line.

- Prefers tabs over commas if more frequent

---

## Duplicate detection (PyArrow)

### `arrow_drop_duplicates(table, pk_names)`

Removes duplicate rows from a PyArrow table based on primary key columns.

- Sorts by PKs
- Keeps the first occurrence
- Handles chunked arrays safely

Used by `ParquetLoader` for internal deduplication.

---

## Batch-oriented CSV parsing

### `conservative_load_parquet(...)`

Despite the name, this helper reads delimited text with PyArrow and yields batches:

- strict column inclusion
- malformed row skipping
- chunked batch iteration

This is used by the PyArrow-based loader path.

---

## PostgreSQL fast-path loading

### `quick_load_pg(...)`

Loads CSV files into a PostgreSQL staging table using `COPY`.

### Characteristics

- Fast
- Bypasses ORM row construction
- Works best on clean input

### Failure handling

- Errors trigger rollback
- `CSVLoadableTableInterface` falls back to ORM-based loading
- Failures are noisy on purpose

This helper is only used when explicitly supported by the database.
