from collections import defaultdict, deque
from collections.abc import Collection
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext import compiler
from sqlalchemy.engine import Engine
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql.compiler import DDLCompiler
from sqlalchemy.sql.selectable import SelectBase

from ..backends.materialized_view_errors import MaterializationOutcome
from ..backends.resolve import resolve_backend
from .materialised_view_contracts import (
    MaterializedViewIndex,
    MaterializedViewSpec,
    _qualified_target,
)


class CreateMaterializedView(DDLElement):
    """
    `CreateMaterializedView`

    SQLAlchemy DDL element representing a CREATE MATERIALIZED VIEW statement.

    This custom DDL construct allows a SQLAlchemy Select construct to be
    compiled into a backend-specific CREATE MATERIALIZED VIEW statement,
    enabling materialized view creation to be expressed using SQLAlchemy's
    DDL execution model.

    Parameters
    ----------
    name
        Unquoted materialized-view name. The compiler quotes it.
    schema
        Optional unquoted schema name. The compiler quotes it separately.
    selectable
        A SQLAlchemy Select construct defining the query backing the
        materialized view.
    with_data
        When False, emits ``WITH NO DATA`` so the view is created empty
        (uninitialised, and not queryable until refreshed).
    if_not_exists
        When True, emit ``IF NOT EXISTS`` so creating an
        already-present view is a no-op rather than an error.
    """

    def __init__(
        self,
        name: str,
        selectable: SelectBase,
        *,
        schema: str | None = None,
        with_data: bool = True,
        if_not_exists: bool = False,
    ) -> None:
        self.name = name
        self.schema = schema
        self.selectable = selectable
        self.with_data = with_data
        self.if_not_exists = if_not_exists

    inherit_cache = False


@compiler.compiles(CreateMaterializedView, "postgresql")
def _create_view(
    element: CreateMaterializedView,
    compiler: DDLCompiler,
    **kwargs: Any
) -> str:

    """
    `_create_view`

    Compile a CreateMaterializedView DDL element into SQL.

    The underlying Select construct is compiled with literal binds so that
    the resulting SQL is fully self-contained and suitable for use in a
    CREATE MATERIALIZED VIEW statement.

    Notes
    -----
    This compiler is backend-specific and assumes support for
    CREATE MATERIALIZED VIEW IF NOT EXISTS syntax (e.g. PostgreSQL).
    """
    compiled = compiler.sql_compiler.process(element.selectable, literal_binds=True)
    existence = "IF NOT EXISTS " if element.if_not_exists else ""
    population = "" if element.with_data else " WITH NO DATA"
    target = _qualified_target(compiler, element.name, element.schema)
    return f"CREATE MATERIALIZED VIEW {existence}{target} AS {compiled}{population}"


class MaterializedViewMixin:

    """
    `MaterializedViewMixin`

    Mixin providing materialized view lifecycle helpers.

    Classes using this mixin must define:

    - ``__mv_name__``: the name of the materialized view
    - ``__mv_select__``: a SQLAlchemy Select defining the view contents
    - ``__mv_logical_identity__``: selected columns defining the complete row grain
    - optionally, ``__mv_dependencies__``: names of tables or materialized views this MV depends on

    This mixin does not define ORM mappings; it is intended for schema-level
    helpers used during migrations, setup, or administrative workflows.

    Examples
    --------
    ```python
    class RecentObservationMV(MaterializedViewMixin):

        __mv_name__ = "mv_recent_observation"
        __mv_logical_identity__ = ("observation_id",)

        __mv_select__ = (
            select(
                Observation.observation_id,
                Observation.person_id,
                Observation.observation_date,
                Observation.value_as_number,
                Concept.concept_id,
                Concept.concept_name,
                Concept.domain_id,
            )
            .join(
                Concept,
                Observation.observation_concept_id == Concept.concept_id
            )
            .where(
                Observation.observation_date
                >= func.current_date() - text("INTERVAL '30 days'")
            )
        )
    ```

    `__mv_select__` is a normal SQLAlchemy Select. No special syntax required.

    By combining with declarative base, you can define columns to query the mv as an object too:

    ```python

    daily_counts_select = (
        select(
            Observation.observation_date.label("observation_date"),
            Observation.observation_concept_id.label("concept_id"),
            sa.func.count().label("n_observations"),
            sa.func.row_number().over().label('mv_id')
        )
        .group_by(
            Observation.observation_date,
            Observation.observation_concept_id,
        )
    )

    class DailyObservationCountsMV(Base, MaterializedViewMixin):

        __mv_name__ = "mv_daily_observation_counts"
        __mv_select__ = daily_counts_select
        __mv_logical_identity__ = ("observation_date", "concept_id")
        __table_args__ = {"extend_existing": True}
        __tablename__ = __mv_name__

        __mv_dependencies__ = {
            "observation",
            "concept",
        }

        mv_id = sa.Column(primary_key=True)
        observation_date = sa.Column(sa.Date, nullable=False)
        concept_id = sa.Column(sa.Integer, nullable=False)
        n_observations = sa.Column(sa.Integer, nullable=False)
        
 
    ```
    Query like a normal mapped class:

    ```python
 
    rows = (
        session.query(DailyObservationCount)
        .filter(DailyObservationCount.observation_date >= date(2025, 1, 1))
        .order_by(DailyObservationCount.n_observations.desc())
        .all()
    )
    ```

    Best practices

    * No inserts / updates
    * Composite PK required for ORM identity map
    * Treat as immutable cache

    """
    __mv_name__: str
    __mv_select__: SelectBase
    __mv_logical_identity__: tuple[str, ...] = ()
    __mv_dependencies__: Collection[str] = ()
    __mv_indexes__: tuple[MaterializedViewIndex, ...] = ()

    @classmethod
    def materialized_view_spec(cls) -> MaterializedViewSpec:
        """Return and validate the side-effect-free definition for this view."""
        dependencies = tuple(cls.__mv_dependencies__)
        if isinstance(cls.__mv_dependencies__, set):
            dependencies = tuple(sorted(dependencies))
        return MaterializedViewSpec(
            name=cls.__mv_name__,
            selectable=cls.__mv_select__,
            logical_identity=cls.__mv_logical_identity__,
            dependencies=dependencies,
            indexes=cls.__mv_indexes__,
        )

    @classmethod
    def create_mv(
        cls,
        bind: "sa.engine.Connection | sa.engine.Engine",
        *,
        schema: str | None = None,
        with_data: bool = True,
        if_not_exists: bool = False,
        create_indexes: bool = True,
    ) -> tuple[MaterializationOutcome, ...]:
        """
        Create the materialized view and its declared indexes.

        Parameters
        ----------
        bind
            A SQLAlchemy Engine or Connection used to execute the DDL.
        schema
            Reserved for qualified-schema integration. Omit it for the
            supported unqualified ``public``-schema contract in this release.
        if_not_exists
            Emit ``IF NOT EXISTS``. The default is False so a changed
            definition cannot be hidden during deployment.
        create_indexes
            When True (the default), also create every index declared in
            ``__mv_indexes__`` immediately after the view. Classes that
            never declare indexes are unaffected: this is then a no-op.

        Notes
        -----
        The underlying SQL is emitted via a custom DDL element and executed
        through the resolved backend. With the built-in backends, this means
        PostgreSQL. Unsupported backends raise
        ``UnsupportedMaterializationDialectError``. Engine inputs run view and
        index creation in one transaction; Connection inputs participate in
        the caller's transaction.


        Examples
        --------

        ```python

        with engine.begin() as conn:
            RecentObservationMV.create_mv(conn)

        ```

        This emits SQL equivalent to:

        ```sql
        CREATE MATERIALIZED VIEW mv_recent_observation AS
        SELECT
            observation.observation_id,
            observation.person_id,
            observation.observation_date,
            observation.value_as_number
        FROM observation
        WHERE observation.observation_date >= CURRENT_DATE - INTERVAL '30 days';
        ```
        """
        if isinstance(bind, Engine):
            # View creation and its declared indexes form one deployment
            # operation. A single owner-managed transaction prevents an index
            # failure from leaving a partially-created view behind.
            with bind.begin() as connection:
                return cls.create_mv(
                    connection,
                    schema=schema,
                    with_data=with_data,
                    if_not_exists=if_not_exists,
                    create_indexes=create_indexes,
                )
        spec = cls.materialized_view_spec()
        backend = resolve_backend(bind)
        outcomes = [backend.create_materialized_view(
            bind,
            spec.name,
            spec.selectable,
            schema=schema,
            with_data=with_data,
            if_not_exists=if_not_exists,
        )]
        if create_indexes:
            outcomes.extend(
                backend.create_materialized_view_index(
                    bind, spec.name, index, schema=schema, if_not_exists=if_not_exists
                )
                for index in spec.indexes
            )
        return tuple(outcomes)

    @classmethod
    def refresh_mv(
        cls,
        bind: "sa.engine.Connection | sa.engine.Engine",
        *,
        schema: str | None = None,
        concurrently: bool = False,
    ) -> MaterializationOutcome:
        """
        Refresh the contents of the materialized view.

        Parameters
        ----------
        bind
            A SQLAlchemy Engine or Connection used to execute the refresh.
        schema
            Reserved for qualified-schema integration. Omit it for the
            supported unqualified ``public``-schema contract in this release.
        concurrently
            Request ``REFRESH MATERIALIZED VIEW CONCURRENTLY``. Requires at
            least one ``unique=True`` entry in ``__mv_indexes__`` that the
            declaration. PostgreSQL remains authoritative for whether that
            index actually exists and satisfies its live prerequisites.

        Notes
        -----
        This method issues a backend-specific refresh statement. With the
        built-in backends, materialized views are PostgreSQL-only.

        Examples
        --------
        ```python
        with engine.begin() as conn:
            RecentObservationMV.refresh_mv(conn)
        ```
        """
        spec = cls.materialized_view_spec()
        backend = resolve_backend(bind)
        return backend.refresh_materialized_view(
            bind,
            spec.name,
            schema=schema,
            concurrently=concurrently,
            declared_indexes=spec.indexes,
        )

    @classmethod
    def drop_mv(
        cls,
        bind: "sa.engine.Connection | sa.engine.Engine",
        *,
        schema: str | None = None,
        if_exists: bool = True,
        cascade: bool = False,
    ) -> MaterializationOutcome:
        """
        Drop the materialized view.

        Parameters
        ----------
        bind
            A SQLAlchemy Engine or Connection used to execute the DDL.
        schema
            Reserved for qualified-schema integration. Omit it for the
            supported unqualified ``public``-schema contract in this release.
        if_exists
            When True (the default), dropping an already-absent view is a
            no-op rather than an error.
        cascade
            Also drop objects that depend on this view.

        Examples
        --------
        ```python
        with engine.begin() as conn:
            RecentObservationMV.drop_mv(conn)
        ```
        """
        spec = cls.materialized_view_spec()
        backend = resolve_backend(bind)
        return backend.drop_materialized_view(
            bind, spec.name, schema=schema, if_exists=if_exists, cascade=cascade
        )


def resolve_mv_refresh_order(
    mv_classes: list[type[MaterializedViewMixin]],
) -> list[type[MaterializedViewMixin]]:
    """
    `resolve_mv_refresh_order`

    Resolve materialized view refresh order using topological sort.

    Raises
    ------
    RuntimeError
        If a dependency cycle is detected.
    """

    specs = [cls.materialized_view_spec() for cls in mv_classes]
    names = tuple(spec.name for spec in specs)
    if len(names) != len(set(names)):
        duplicates = sorted(name for name in set(names) if names.count(name) > 1)
        raise ValueError(f"materialized view names must be unique: {duplicates}")
    name_to_mv = {spec.name: cls for spec, cls in zip(specs, mv_classes, strict=True)}

    graph: dict[str, set[str]] = defaultdict(set)
    indegree: dict[str, int] = defaultdict(int)

    for spec in specs:
        indegree.setdefault(spec.name, 0)

        for dep in spec.dependencies:
            # Only track dependencies that are themselves MVs
            if dep in name_to_mv:
                graph[dep].add(spec.name)
                indegree[spec.name] += 1

    queue = deque(
        name for name, deg in indegree.items() if deg == 0
    )

    ordered: list[str] = []

    while queue:
        node = queue.popleft()
        ordered.append(node)

        for downstream in graph[node]:
            indegree[downstream] -= 1
            if indegree[downstream] == 0:
                queue.append(downstream)

    if len(ordered) != len(indegree):
        raise RuntimeError(
            "Cycle detected in materialized view dependencies"
        )

    return [name_to_mv[name] for name in ordered]


def refresh_all_mvs(
    bind: "sa.engine.Connection | sa.engine.Engine",
    mv_classes: list[type[MaterializedViewMixin]],
) -> tuple[MaterializationOutcome, ...]:

    """
    `refresh_all_mvs`
    
    Handle refreshing multiple materialized views in dependency order.

    Examples
    --------
    ```python
        ALL_MVS = [
            ObservationWithConceptMV,
            DailyObservationCountsMV,
        ]

        refresh_all_mvs(engine, ALL_MVS)
    ```
    """
    ordered = resolve_mv_refresh_order(mv_classes)

    return tuple(mv.refresh_mv(bind) for mv in ordered)
