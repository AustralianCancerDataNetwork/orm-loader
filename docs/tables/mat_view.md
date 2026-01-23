# Materialised Views

This module provides a SQLAlchemy-native pattern for defining, creating, refreshing, and orchestrating materialized views using normal `Select` constructs, with explicit dependency management and deterministic refresh order.


It is designed for:

* analytics and reporting layers
* large fact tables with repeated joins or aggregates
* schema-level orchestration (migrations, setup, Airflow, admin tasks)

The implementation is PostgreSQL-oriented (due to materialized view support), but remains cleanly isolated from ORM persistence logic.

## Overview

The materialized view system consists of four main parts:

1. `CreateMaterializedView`: A custom SQLAlchemy DDLElement that compiles a Select into a `CREATE MATERIALIZED VIEW IF NOT EXISTS` statement.
2. `MaterializedViewMixin`: A mixin used to define materialized views declaratively, including:
    * name
    * backing `Select`
    * optional dependencies
3. Dependency resolution: A topological sort over declared dependencies to determine refresh order.
4. Refresh orchestration: Helpers to refresh one or many materialized views safely and predictably.


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
