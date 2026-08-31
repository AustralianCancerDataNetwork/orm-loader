# Materialised Views

The public `orm_loader.materialized_views` module provides a SQLAlchemy-native
way to define, create, refresh, drop, index, and order materialized views from
ordinary `Select` constructs.


It is designed for:

* analytics and reporting layers
* large fact tables with repeated joins or aggregates
* schema-level orchestration (migrations, setup, Airflow, admin tasks)

Materialized-view lifecycle operations require PostgreSQL. This release is
validated for unqualified materialized views in the `public` schema. Use a
connection whose active schema resolves to `public`; qualified-schema support
is outside this release contract.

## Overview

The materialized view system consists of four main parts:

1. `MaterializedViewSpec`: an immutable, side-effect-free definition with a
   name, selectable, logical row identity, dependencies, and indexes.
2. `MaterializedViewMixin`: a mixin used to define and operate materialized
   views declaratively, including:
    * name
    * backing `Select`
    * complete logical row identity (`__mv_logical_identity__`)
    * optional dependencies
    * optional declared indexes (`__mv_indexes__`)
3. Dependency resolution: A topological sort over declared dependencies to determine refresh order.
4. Refresh orchestration: Helpers to refresh one or many materialized views in a predictable order.

Use only the public module in application code:

```python
from orm_loader.materialized_views import (
    MaterializedViewIndex,
    MaterializedViewMixin,
)
```

## Identity, indexes, and concurrent refresh

`__mv_logical_identity__` declares the complete selected columns that
distinguish rows in the view. Definitions fail before database execution when
the identity is empty, contains duplicates, or references a column that the
selectable does not expose. Index declarations receive the same selected-column
validation, and duplicate index names or dependency declarations are rejected.

Logical identity is descriptive metadata, not a database constraint. The
application that owns a materialized view remains responsible for testing its
uniqueness and declaring a matching unique index when concurrent refresh is
required.

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
Successful refresh and drop calls return a `MaterializationOutcome`;
`create_mv` returns one outcome for the view followed by one for each declared
index. Database failures during create, refresh, drop, or index creation are wrapped in a
`MaterializationError` with the original exception preserved as `__cause__`,
so callers can catch on operation/target context without losing the
underlying database error. SQLite raises
`UnsupportedMaterializationDialectError` before issuing lifecycle SQL.

Creation defaults to failing when the target or index already exists.
Applications should perform explicit replacement/rebuild decisions rather than
using `IF NOT EXISTS` to hide definition drift. Set `if_not_exists=True` only
when an idempotent no-op is the intended deployment policy.

### Defining the Materialised View

::: orm_loader.materialized_views.MaterializedViewMixin
    options:
      heading_level: 3
      members: true
      

::: orm_loader.materialized_views.MaterializedViewSpec
    options:
      heading_level: 3
      members: true

::: orm_loader.materialized_views.resolve_mv_refresh_order
    options:
      heading_level: 3
      members: true

::: orm_loader.materialized_views.refresh_all_mvs
    options:
      heading_level: 3
      members: true

::: orm_loader.materialized_views.MaterializedViewIndex
    options:
      heading_level: 3
      members: true

::: orm_loader.materialized_views.MaterializationOutcome
    options:
      heading_level: 3
      members: true

::: orm_loader.materialized_views.MaterializationError
    options:
      heading_level: 3
      members: true

::: orm_loader.materialized_views.ConcurrentRefreshNotEligibleError
    options:
      heading_level: 3

::: orm_loader.materialized_views.UnsupportedMaterializationDialectError
    options:
      heading_level: 3
