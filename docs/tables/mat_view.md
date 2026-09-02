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

Every lifecycle method (`create_mv`, `refresh_mv`, `drop_mv`) accepts an optional `schema` override. It defaults to `None`, which leaves the target name unqualified for the connection's `search_path` to resolve. Passing an explicit schema uses a quoted schema-qualified target; this API does not provide multi-schema or role-token support.

`__mv_indexes__` declares simple column indexes (`MaterializedViewIndex`) created immediately after the view by `create_mv`. Declaring at least one `unique=True` index is a prerequisite for `refresh_mv(concurrently=True)`: PostgreSQL requires a matching unique index for concurrent refresh. The declaration is checked in memory before issuing SQL, while PostgreSQL's own prerequisite rejection is translated into `ConcurrentRefreshNotEligibleError` with the original error preserved as `__cause__`.

`drop_mv` supports `if_exists` and `cascade`. Any failure in `drop_mv` or index creation is wrapped in `MaterializationError` with the original exception preserved as `__cause__`. SQLite continues to reject every materialized-view operation outright.


### Defining the Materialised View

::: orm_loader.mappers.materialised_view_mixin.CreateMaterializedView
    options:
      heading_level: 3

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
