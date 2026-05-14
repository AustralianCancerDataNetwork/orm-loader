# Loader Implementations

This page documents the concrete loader implementations provided by
`orm_loader`.

All loaders implement the same interface. The difference is in how they read data and how much work they do before rows reach the staging table.

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
- leave final merge behaviour to the table layer

---

## PandasLoader

`PandasLoader` uses pandas to read and process files.

### Characteristics

- Works well with CSV and TSV inputs
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

### Best suited for

- high-volume ingestion
- repeated production loads
- columnar data sources

---

## Deduplication behaviour

Deduplication here means deduplicating within the incoming data before it is inserted into staging. The merge step is what decides what happens when incoming rows overlap with existing target rows.

---

## Normalisation behaviour

When enabled, loaders:

- cast values to ORM column types
- drop rows violating required constraints
- log casting failures with examples

No schema changes are performed at the loader layer.
