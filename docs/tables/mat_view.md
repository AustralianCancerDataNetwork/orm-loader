# Materialised Views

This module provides a SQLAlchemy-native way to define, create, refresh, and order materialized views from ordinary `Select` constructs.


It is designed for:

* analytics and reporting layers
* large fact tables with repeated joins or aggregates
* schema-level orchestration (migrations, setup, Airflow, admin tasks)

The implementation is PostgreSQL-oriented. The mixin resolves a backend from the supplied bind, and the built-in PostgreSQL backend is currently the only one that supports materialized views.

## Overview

The materialized view system consists of four main parts:

1. `CreateMaterializedView`: A custom SQLAlchemy DDLElement that compiles a Select into a `CREATE MATERIALIZED VIEW IF NOT EXISTS` statement.
2. `MaterializedViewMixin`: A mixin used to define materialized views declaratively, including:
    * name
    * backing `Select`
    * optional dependencies
    * optional declared indexes (`__mv_indexes__`)
3. Dependency resolution: A topological sort over declared dependencies to determine refresh order.
4. Refresh orchestration: Helpers to refresh one or many materialized views in a predictable order.

## Schema, indexes, and concurrent refresh

Every lifecycle method (`create_mv`, `refresh_mv`, `drop_mv`) accepts an
optional `schema` override. When omitted, the target schema is resolved
from the bind's own `schema_translate_map` (via `oa_configurator.schema_of`)
rather than needing to be threaded through by hand at every call site — an
unconfigured bind resolves to the same unqualified name these methods have
always used, so existing callers are unaffected.

`__mv_indexes__` declares simple column indexes (`MaterializedViewIndex`)
created immediately after the view by `create_mv`. Declaring at least one
`unique=True` index is a prerequisite for `refresh_mv(concurrently=True)`:
PostgreSQL itself requires a matching unique index to support
`REFRESH MATERIALIZED VIEW CONCURRENTLY`. `refresh_mv` checks for that
declaration cheaply and in-memory — no database round trip — and raises
`ConcurrentRefreshNotEligibleError` immediately if nothing is declared at
all. Whether a declared index genuinely exists in this database (as opposed
to a stale declaration whose `create_mv()` step never ran, or an index
someone dropped later) is PostgreSQL's own prerequisite, checked by
PostgreSQL itself when the refresh is attempted: rather than re-deriving
that same rule against the live catalog ourselves, the backend attempts the
refresh and translates PostgreSQL's own rejection into the same
`ConcurrentRefreshNotEligibleError`, with the original error preserved as
`__cause__`.

`drop_mv` supports the same `if_exists`/`cascade` options as the DDL itself.
Any failure in `drop_mv` or index creation is wrapped in a
`MaterializationError` with the original exception preserved as `__cause__`,
so callers can catch on operation/target context without losing the
underlying database error. SQLite continues to reject every materialized-view
operation outright.

### Defining the Materialised View

::: orm_loader.mappers.materialised_view_mixin.CreateMaterializedView
    options:
      heading_level: 3

::: orm_loader.mappers.materialised_view_mixin.MaterializedViewMixin
    options:
      heading_level: 3
      members: true
      

::: orm_loader.mappers.materialised_view_mixin.resolve_mv_refresh_order
    options:
      heading_level: 3
      members: true

::: orm_loader.mappers.materialised_view_mixin.refresh_all_mvs
    options:
      heading_level: 3
      members: true

::: orm_loader.mappers.materialised_view_contracts.MaterializedViewIndex
    options:
      heading_level: 3
      members: true

::: orm_loader.mappers.materialised_view_contracts.CreateMaterializedViewIndex
    options:
      heading_level: 3

::: orm_loader.mappers.materialised_view_contracts.DropMaterializedView
    options:
      heading_level: 3

::: orm_loader.backends.materialized_view_errors.MaterializationError
    options:
      heading_level: 3
      members: true

::: orm_loader.backends.materialized_view_errors.ConcurrentRefreshNotEligibleError
    options:
      heading_level: 3

::: orm_loader.backends.materialized_view_errors.UnsupportedMaterializationDialectError
    options:
      heading_level: 3
