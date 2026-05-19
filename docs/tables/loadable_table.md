# Loadable Table Mixins

Infrastructure for staged, file-based ingestion into ORM tables.

Supports:
- staged file loading into backend-specific staging tables
- PostgreSQL fast-path `COPY` with ORM fallback
- backend-aware merge strategies
- pandas and PyArrow-based loader paths
- index handling during merge

---

## CSVLoadableTableInterface

::: orm_loader.tables.loadable_table.CSVLoadableTableInterface
