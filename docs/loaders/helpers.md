# Loader Helper Utilities

This page documents low-level helper functions used by loaders.

These utilities are stateless and intentionally conservative.

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

## Conservative CSV parsing

### `conservative_load_parquet(...)`

Reads CSV files using PyArrow with:

- strict column inclusion
- malformed row skipping
- chunked batch iteration

This is used when loading CSVs via the Parquet pipeline.

---

## PostgreSQL fast-path loading

### `quick_load_pg(...)`

Loads CSV files into PostgreSQL staging tables using `COPY`.

### Characteristics

- Extremely fast
- Bypasses ORM
- Sensitive to data quality issues

### Failure handling

- Errors trigger rollback
- Loader falls back to ORM-based loading
- No partial silent loads

This helper is only used when explicitly supported by the database.
