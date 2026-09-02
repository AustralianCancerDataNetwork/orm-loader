# Materialised Views

Materialized views are database-maintained read models: a stored `Select` whose result can be queried like a table and rebuilt when its source data changes. They are useful when a downstream application repeatedly performs the same expensive join, filter, or aggregation.

This page describes the contract that downstream packages should depend on. The implementation lives in `orm_loader`; consumers should declare a view once and call its lifecycle methods rather than reproducing the DDL themselves.

## The ownership boundary

`orm_loader` owns the physical materialized-view lifecycle:

1. create the view from `__mv_select__`;
2. create its declared indexes;
3. refresh views in dependency order; and
4. drop views during teardown or replacement.

The consumer owns when those operations happen. A typical application creates views during database setup, loads or updates its source tables, and refreshes the views as part of the same operational workflow. A view is not a second source of truth: its `Select` and declarations in code are the source of truth, while the database object is a refreshable read cache.

This is deliberately separate from the ordinary [ORM table mixins](orm_table.md) and from the [loader lifecycle](../loaders/index.md). Loaders populate source tables; materialized-view orchestration makes derived read models available after that work has completed.

## Declare the read model once

The class declaration is the integration point that downstream code can import and reuse. It names the physical view, defines its contents, records materialized-view dependencies, and optionally declares indexes.

```python
class RecentObservationMV(Base, MaterializedViewMixin):
    __mv_name__ = "mv_recent_observation"
    __mv_select__ = (
        sa.select(
            Observation.observation_id,
            Observation.person_id,
            Observation.observation_date,
            Observation.value_as_number,
        )
        .where(
            Observation.observation_date
            >= sa.func.current_date() - sa.text("INTERVAL '30 days'")
        )
    )
    __mv_dependencies__ = {"observation"}
    __mv_indexes__ = (
        MaterializedViewIndex(
            name="mv_recent_observation_id_uq",
            columns=("observation_id",),
            unique=True,
        ),
    )
```

The mixin can also be used without an ORM mapping when the view is consumed through SQLAlchemy Core. When it is combined with a declarative base, the class can additionally describe how application code queries the resulting rows. In both cases, keep the physical name and lifecycle declaration in one shared class so setup code and query code cannot drift apart.

`__mv_dependencies__` may contain source-table names as documentation. Only names that belong to the list passed to `resolve_mv_refresh_order` participate in the topological sort. This means a source table does not need to be registered as a materialized view, while dependencies between registered materialized views are refreshed in the correct order.

## A safe lifecycle

For a collection of views, make the refresh list explicit and share it between setup and refresh jobs:

```python
ALL_MVS = [RecentObservationMV, DailyObservationCountsMV]

for view in ALL_MVS:
    view.create_mv(engine)

# Run after the source-table load or update has committed.
refresh_all_mvs(engine, ALL_MVS)
```

`create_mv()` is idempotent with its default `if_not_exists=True`, but it does not compare or migrate an existing definition. If `__mv_select__` changes, the consumer must choose a replacement/migration strategy; calling `create_mv()` again will not rewrite the existing view.

`refresh_all_mvs()` is the useful boundary for downstream orchestration. It sorts the supplied classes and raises `RuntimeError` for a dependency cycle. Refreshing one class directly is appropriate when the caller already knows that its prerequisites are current.

## Choose creation and refresh semantics deliberately

| Operation | Default | Consumer decision |
| --- | --- | --- |
| `create_mv()` | Create and populate the view; create declared indexes | Use `with_data=False` when population should happen in a later refresh; use `create_indexes=False` only when index creation is managed elsewhere |
| `refresh_mv()` | Blocking refresh | Use `concurrently=True` only when the declaration includes a simple unique index that is created in the target database |
| `drop_mv()` | `IF EXISTS`, no cascade | Use `cascade=True` only when dependent database objects should be removed as part of teardown |

`with_data=False` leaves a newly created view uninitialized. It must be refreshed before consumers query it. `if_not_exists=False` and `drop_mv(if_exists=False)` are useful when setup should fail loudly instead of treating an existing or missing object as acceptable.

The lower-level [DDL contracts](#api-reference) are available for migration systems that need to compose statements themselves, but most downstream code should use the mixin methods so indexes and refresh eligibility remain tied to the declaration.

## Concurrent refresh is a declaration-and-database contract

PostgreSQL permits `REFRESH MATERIALIZED VIEW CONCURRENTLY` only when the view has a suitable simple unique index. Declare that index with `MaterializedViewIndex(unique=True)` in `__mv_indexes__`:

```python
class PatientSummaryMV(MaterializedViewMixin):
    __mv_name__ = "mv_patient_summary"
    __mv_select__ = patient_summary_select
    __mv_indexes__ = (
        MaterializedViewIndex(
            name="mv_patient_summary_patient_id_uq",
            columns=("patient_id",),
            unique=True,
        ),
    )


PatientSummaryMV.create_mv(engine)
PatientSummaryMV.refresh_mv(engine, concurrently=True)
```

`create_mv()` creates declared indexes immediately after the view. The `concurrently=True` pre-check is intentionally cheap and conservative: it checks the declaration before issuing SQL, then lets PostgreSQL verify that the index really exists and is valid. If no unique index is declared, or if PostgreSQL rejects the concurrent refresh, the operation raises `ConcurrentRefreshNotEligibleError` without hiding the underlying cause.

This is fail-closed by design. An index created manually outside `__mv_indexes__` does not satisfy the mixin's declaration contract; declare it in the class even if another migration is responsible for creating it. Expressions, partial indexes, and other index forms are outside this simple contract and should not be represented as `MaterializedViewIndex` entries.

By default, `schema=None` leaves the view name unqualified. PostgreSQL resolves that name through the connection's `search_path`, matching the behavior of existing callers. Pass `schema="reporting"` only when the caller intentionally wants an explicit schema-qualified target.

```python
# Existing/default behavior: search_path resolves the target.
RecentObservationMV.create_mv(engine)

# Explicit schema: the target is quoted and schema-qualified.
RecentObservationMV.create_mv(engine, schema="reporting")
RecentObservationMV.refresh_mv(engine, schema="reporting")
```

Explicit schema targets are quoted component by component. This matters for embedded quotes, spaces, and mixed-case identifiers. It also means an unqualified mixed-case name and the same name passed with `schema=` can address different PostgreSQL relations. Keep schema selection at the call site and do not assume that this API provides `schema_translate_map`, role-token, or general multi-schema behavior.

## Failure handling and backend support

The built-in implementation is PostgreSQL-oriented. SQLite rejects materialized-view operations with `NotImplementedError`; this is intentional, not an emulation using ordinary views.

`drop_mv()` and declared-index creation wrap execution failures in `MaterializationError`. 

## API reference

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
