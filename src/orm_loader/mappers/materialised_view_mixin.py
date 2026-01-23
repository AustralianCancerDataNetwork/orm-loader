from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
import sqlalchemy as sa
from collections import defaultdict, deque

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
        Name of the materialized view to be created.
    selectable
        A SQLAlchemy Select construct defining the query backing the
        materialized view.
    """

    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable

@compiler.compiles(CreateMaterializedView)
def _create_view(element, compiler, **kw):

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
    return f"CREATE MATERIALIZED VIEW IF NOT EXISTS {element.name} as {compiled}"

class MaterializedViewMixin:

    """
    `MaterializedViewMixin`

    Mixin providing materialized view lifecycle helpers.

    Classes using this mixin must define:

    - ``__mv_name__``: the name of the materialized view
    - ``__mv_select__``: a SQLAlchemy Select defining the view contents
    - optionally, ``__mv_dependencies__``: names of tables or materialized views this MV depends on

    This mixin does not define ORM mappings; it is intended for schema-level
    helpers used during migrations, setup, or administrative workflows.

    Examples
    --------
    ```python
    class RecentObservationMV(MaterializedViewMixin):

        __mv_name__ = "mv_recent_observation"

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
        __mv_pk__ = ["mv_id"]
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
    __mv_select__: sa.sql.Select
    __mv_dependencies__: set[str] = set()

    @classmethod
    def create_mv(cls, bind):
        """
        Create the materialized view if it does not already exist.

        Parameters
        ----------
        bind
            A SQLAlchemy Engine or Connection used to execute the DDL.

        Notes
        -----
        The underlying SQL is emitted via a custom DDL element and executed
        directly against the database. This operation is not transactional
        on all backends.


        Examples
        --------

        ```python

        with engine.begin() as conn:
            RecentObservationMV.create_mv(conn)
        
        ```

        This emits SQL equivalent to:

        ```sql
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_recent_observation AS
        SELECT
            observation.observation_id,
            observation.person_id,
            observation.observation_date,
            observation.value_as_number
        FROM observation
        WHERE observation.observation_date >= CURRENT_DATE - INTERVAL '30 days';
        ```
        """
        ddl = CreateMaterializedView(cls.__mv_name__, cls.__mv_select__)
        bind.execute(ddl)

    @classmethod
    def refresh_mv(cls, bind):
        """
        Refresh the contents of the materialized view.

        Parameters
        ----------
        bind
            A SQLAlchemy Engine or Connection used to execute the refresh.

        Notes
        -----
        This method issues a REFRESH MATERIALIZED VIEW statement and assumes
        backend support (e.g. PostgreSQL). Concurrent refresh semantics are
        not handled here.

        Examples
        --------
        ```python        
        with engine.begin() as conn:
            RecentObservationMV.refresh_mv(conn)
        ```
        """
        bind.execute(sa.text(f"REFRESH MATERIALIZED VIEW {cls.__mv_name__};"))
        

def resolve_mv_refresh_order(mv_classes: list[type[MaterializedViewMixin]]) -> list[type]:
    """
    `resolve_mv_refresh_order`

    Resolve materialized view refresh order using topological sort.

    Raises
    ------
    RuntimeError
        If a dependency cycle is detected.
    """

    name_to_mv = {cls.__mv_name__: cls for cls in mv_classes}

    graph: dict[str, set[str]] = defaultdict(set)
    indegree: dict[str, int] = defaultdict(int)

    for cls in mv_classes:
        indegree.setdefault(cls.__mv_name__, 0)

        for dep in cls.__mv_dependencies__:
            # Only track dependencies that are themselves MVs
            if dep in name_to_mv:
                graph[dep].add(cls.__mv_name__)
                indegree[cls.__mv_name__] += 1

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


def refresh_all_mvs(bind, mv_classes):

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

    for mv in ordered:
        mv.refresh_mv(bind)