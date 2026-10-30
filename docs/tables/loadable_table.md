# Loadable Table Mixins

Infrastructure for staged, file-based ingestion into ORM tables.

Supports:
- staged file loading into backend-specific staging tables
- bulk staging sinks per backend: PostgreSQL `COPY`, DuckDB insert from Arrow, SQLite `executemany`
- backend-aware merge strategies
- Arrow-based file readers (delimited text and Parquet)
- index handling during merge

---

## FileLoadableTableInterface

::: orm_loader.tables.loadable_table.FileLoadableTableInterface
