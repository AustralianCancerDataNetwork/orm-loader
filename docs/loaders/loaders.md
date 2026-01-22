# Loader Implementations

This page documents the concrete loader implementations provided by
`orm_loader`.

All loaders implement the same interface and differ only in
how data is read and processed.

---

## LoaderInterface

`LoaderInterface` defines the contract for all loaders.

### Required methods

- `orm_file_load(ctx)`
- `dedupe(data, ctx)`

### Shared behaviour

All loaders:

- load into staging tables only
- respect `LoaderContext` flags
- return row counts
- avoid implicit commits

---

## PandasLoader

`PandasLoader` uses pandas to read and process files.

### Characteristics

- Supports CSV and TSV inputs
- Easy to debug and inspect
- Supports chunked loading
- Flexible transformation pipeline

### Trade-offs

- Slower for very large datasets
- Higher memory overhead than columnar approaches

### Best suited for

- initial data exploration
- messy or inconsistent files
- pipelines requiring heavy cleaning or inspection

---

## ParquetLoader

`ParquetLoader` uses PyArrow for columnar ingestion.

### Characteristics

- Efficient for very large datasets
- Supports Parquet and CSV inputs
- Batch-oriented processing
- Lower memory overhead

### Trade-offs

- More complex pipeline
- Less flexible row-wise transformations
- DB-level deduplication not yet implemented

### Best suited for

- high-volume ingestion
- repeated production loads
- columnar data sources

---

## Deduplication behaviour

Deduplication occurs in two phases:

1. **Internal deduplication**  
   Removes duplicate primary key rows within the incoming data.

2. **Database-level deduplication (optional)**  
   Removes rows that already exist in the database.

Database-level deduplication is currently implemented for pandas-based
loads.

---

## Normalisation behaviour

When enabled, loaders:

- cast values to ORM column types
- drop rows violating required constraints
- log casting failures with examples

No schema changes are performed.
