from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Collection, Generator, Iterable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, ClassVar, ParamSpec, Type, TypeVar, cast

import pyarrow as pa
import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import (
    Dialect,
    ResolvedCDMDatabase,
    ResolvedDatabase,
    Role,
    SchemaClaim,
    is_ephemeral_url,
    open_connection,
    qualified,
    unenforced_foreign_keys,
)
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.sql.compiler import IdentifierPreparer

if TYPE_CHECKING:
    from ..loaders.loader import LoaderContext
    from ..mappers.materialised_view_contracts import MaterializedViewIndex
    from ..tables.typing import FileTableProtocol


@dataclass(frozen=True)
class DedupeResult:
    """What deduplicating a staging table removed.

    Attributes
    ----------
    key_columns
        Primary key columns that identify a duplicate.
    rows_before
        Rows in the staging table before deduplication.
    rows_removed
        Rows removed because an earlier row had the same key.
    """

    key_columns: tuple[str, ...]
    rows_before: int
    rows_removed: int

    @property
    def rows_after(self) -> int:
        """Rows left in the staging table."""
        return self.rows_before - self.rows_removed


@dataclass(frozen=True)
class BackendCapabilities:
    """
    Capability flags exposed by a database backend.

    These defaults are intentionally conservative. Concrete backends should
    opt into capabilities explicitly.
    """

    supports_unlogged_staging: bool = False
    supports_fk_toggle: bool = False
    supports_materialized_views: bool = False
    supports_replace_with_kept_indexes: bool = False


class ForeignKeyViolationError(RuntimeError):
    """Rows reference a row that does not exist in the referred table.

    Parameters
    ----------
    constraint
        The violated foreign key.
    orphan_keys
        Up to five referencing key values that have no match.
    """

    qualifier: ClassVar[str] = ""

    def __init__(self, constraint: sa.ForeignKeyConstraint, orphan_keys: list[tuple[Any, ...]]) -> None:
        self.constraint = constraint
        self.orphan_keys = orphan_keys
        local_cols = ", ".join(c.name for c in constraint.columns)
        referred_table = next(iter(constraint.elements)).column.table.name
        super().__init__(
            f"{constraint.table.name}.({local_cols}) references {referred_table!r}{self.qualifier}; "
            f"{len(orphan_keys)} row(s) violate it (showing up to 5): {orphan_keys}"
        )


class DeferredForeignKeyViolationError(ForeignKeyViolationError):
    """A foreign key this backend can't enforce at the DB level is violated."""

    qualifier = " via a constraint this backend can't enforce at the DB level"


STAGING_SCHEMA: str = "staging"


def staging_schema_claim(physical_schema: str | None = None, *, reserved: bool = True) -> SchemaClaim:
    """SchemaClaim reserving orm-loader's staging schema.
    Since orm-loader doesn't have an engine creation step of its own,
    this claim needs to be registered by the caller ingesting tables into 
    the database through orm-loader.

    Add to your own ``create_engines(schema_claims=[...])`` call before
    handing a session into this package's staging path.

    Parameters
    ----------
    physical_schema : str, optional
        Physical schema to reserve. Defaults to ``STAGING_SCHEMA`` itself.
    reserved : bool, optional
        Whether no other owner may claim this physical schema on the same
        connection. Defaults to True.
    """
    return SchemaClaim(
        schema_tag=STAGING_SCHEMA,
        physical_schema=physical_schema or STAGING_SCHEMA,
        reserved=reserved,
        owner="orm_loader",
    )


P = ParamSpec("P")
R = TypeVar("R")


def requires_capability(
    capability_name: str,
    feature_name: str,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Guard a concrete backend method with a capability requirement.

    Subclass overrides must apply this decorator themselves; Python does not
    automatically retain decorators when a method is overridden.
    """

    def decorator(method: Callable[P, R]) -> Callable[P, R]:
        @wraps(method)
        def guarded(*args: P.args, **kwargs: P.kwargs) -> R:
            # ParamSpec keeps the complete method signature, but type checkers
            # cannot infer that args[0] is the backend instance here.
            self = cast("DatabaseBackend", args[0])
            self._require_capability(capability_name, feature_name)
            return method(*args, **kwargs)

        return guarded

    return decorator


class DatabaseBackend(ABC):
    """
    Abstract base class for database-specific loader behavior.

    This class defines the stable contract for future backend implementations
    without changing existing loader orchestration yet.
    """

    def __init__(
        self,
        *,
        staging_schema_tag: str | None = None,
        staging_schema: str | None = None,
        resolved: ResolvedDatabase | None = None,
        physical_schemas: Mapping[str, str | None] | None = None,
    ) -> None:
        """
        Parameters
        ----------
        staging_schema_tag
            schema_translate_map tag the staging schema was resolved from.
            ``None`` means no schema qualification was requested at all.
            Stored so a caller re-resolving the backend later (e.g. after
            the engine's claims may have changed) re-derives the same tag
            rather than reusing an already-resolved physical name.
        staging_schema
            Already-resolved physical schema staging tables are created in,
            via ``resolve_backend()``. ``None`` means no schema
            qualification — staging tables land in whatever schema the
            connection's search_path resolves to. SQLite has no schema
            concept and always resolves to ``None``.
        resolved
            Used by DuckDBBackend to classify a foreign key crossing a
            schema or database boundary, via oa-configurator's
            ``unenforced_foreign_keys()``. Ignored by every other backend.
        physical_schemas
            A schema tag's physical name beyond ``resolved``'s own
            configured ones (e.g. one claimed via ``schema_claims``).
            Forwarded alongside ``resolved``.
        """
        self.staging_schema_tag = staging_schema_tag
        self.staging_schema = staging_schema
        self.resolved = resolved
        self.physical_schemas = physical_schemas

    @staticmethod
    def _validate_merge_batch_size(merge_batch_size: int | None) -> None:
        if merge_batch_size is not None and merge_batch_size < 1:
            raise ValueError("merge_batch_size must be a positive integer or None")

    @staticmethod
    @abstractmethod
    def staging_name_for_table(tablename: str) -> str:
        """
        Return the unqualified staging table name for a given target tablename.

        Parameters
        ----------
        tablename
            The target table's ``__tablename__``.

        Returns
        -------
        str
            The staging table name (no schema prefix).
        """

    def qualified_staging_name(self, tablename: str) -> str:
        """
        Return the fully schema-qualified staging identifier.

        Parameters
        ----------
        tablename
            The target table's ``__tablename__``.

        Returns
        -------
        str
            e.g. '"staging"."_staging_concept"' or '"_staging_concept"'.
        """
        return qualified(
            self.identifier_preparer, self.staging_name_for_table(tablename), physical_schema=self.staging_schema
        )

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable backend name."""

    @property
    @abstractmethod
    def dialect(self) -> Dialect:
        """SQLAlchemy dialect handled by this backend."""

    @property
    @abstractmethod
    def identifier_preparer(self) -> IdentifierPreparer:
        """The dialect-specific identifier preparer used to quote/escape SQL identifiers."""

    @property
    @abstractmethod
    def capabilities(self) -> BackendCapabilities:
        """Capability flags supported by this backend."""

    def supports_dialect(self, dialect: Dialect) -> bool:
        """Return ``True`` when the backend handles the given dialect."""
        return self.dialect == dialect

    @property
    def default_index_strategy(self) -> str:
        """Default index strategy used when callers request ``auto``."""
        return "drop_rebuild"

    def resolve_index_strategy(self, index_strategy: str) -> str:
        """
        Resolve a caller-facing index strategy to a concrete backend choice.
        """
        valid = {"auto", "drop_rebuild", "keep"}
        if index_strategy not in valid:
            raise ValueError(
                f"Unknown index_strategy '{index_strategy}'. Expected one of: {sorted(valid)}"
            )
        if index_strategy == "auto":
            return self.default_index_strategy
        return index_strategy

    def _require_capability(self, capability_name: str, feature_name: str) -> None:
        """
        Raise a clear error when a backend capability is not supported.
        """
        if not hasattr(self.capabilities, capability_name):
            raise AttributeError(
                f"Unknown backend capability {capability_name!r} on {type(self.capabilities).__name__}"
            )
        if not getattr(self.capabilities, capability_name):
            raise NotImplementedError(
                f"Backend '{self.name}' does not support {feature_name}"
            )
        
    @contextmanager
    def _as_connection(
        self,
        bind: Engine | Connection,
    ) -> Generator[Connection, None, None]:
        """
        Normalize a bind into an open connection, guarding its dialect.

        Every backend method that takes a ``bind`` should route it through
        this context manager rather than opening a connection itself, so the
        dialect guard below applies uniformly.

        Parameters
        ----------
        bind : Engine or Connection
            An Engine opens a new connection and transaction scoped to this
            context manager, committing on a clean exit. A Connection is
            forwarded as-is; its transaction is owned by the caller, and
            passing the same Connection into several backend calls groups
            them into one shared transaction.

        Yields
        ------
        Connection
            An open connection whose dialect matches ``self.dialect``.

        Raises
        ------
        TypeError
            If ``bind``'s dialect does not match ``self.dialect``. Guards
            against a bind resolved through a different backend being
            passed directly into a method on this one.
        """
        if bind.dialect.name != self.dialect.value:
            raise TypeError(
                f"{self.name} backend received a {bind.dialect.name!r} connection; "
                f"expected {self.dialect.value!r}. The bind passed to this method must "
                "be the same one (or share the same dialect as) the bind resolve_backend() "
                "was given."
            )
        with open_connection(bind) as conn:
            yield conn

    def _insertable_column_names(
        self,
        table_cls: Type["FileTableProtocol"],
        staging: sa.Table | None = None,
    ) -> list[str]:
        """
        Return column names safe to include in generic insert statements.

        Computed columns are excluded because backend loaders and merge helpers
        should not attempt to write to them directly. With a staging table,
        only the columns it has are returned, so a column the load left out
        (an autoincrement key) is assigned by the database.
        """
        return [
            c.name
            for c in table_cls.__table__.columns
            if c.computed is None and (staging is None or c.name in staging.c)
        ]

    def deferred_fk_constraints(
        self,
        table_cls: Type["FileTableProtocol"],
    ) -> tuple[sa.ForeignKeyConstraint, ...]:
        """FK constraints on *table_cls* that this backend never enforces at the DB level.

        Default: none. Only a backend whose dialect can't execute a deferred
        ``ALTER TABLE ADD CONSTRAINT`` (DuckDB, for a cyclic FK graph) overrides this.
        """
        return ()

    def validate_deferred_fks(
        self,
        table_cls: Type["FileTableProtocol"],
        session: so.Session,
    ) -> None:
        """Raise if any row in *table_cls* violates one of its deferred_fk_constraints().

        An anti-join check recovering referential-integrity validation this
        backend's DB can't give for those constraints. No-op when there are
        none (every backend but DuckDB, today).

        Raises
        ------
        DeferredForeignKeyViolationError
            If any row's FK column is non-null with no matching row in the
            referenced table.
        """
        for fk in self.deferred_fk_constraints(table_cls):
            self._validate_one_fk(fk, session, DeferredForeignKeyViolationError)

    def validate_foreign_keys(
        self,
        table_cls: Type["FileTableProtocol"],
        session: so.Session,
    ) -> None:
        """Raise if any row in *table_cls* references a row that does not exist.

        Checks every foreign key of the table with an anti-join, whether or
        not the database enforces it. Loads relax foreign keys while merging
        on backends that can toggle them, so this is how referential
        integrity is verified once all tables are loaded, in any load order.
        Null references are not violations.

        Raises
        ------
        ForeignKeyViolationError
            If any row's FK columns are non-null with no matching row in the
            referred table.
        """
        own_fks = tuple(table_cls.__table__.foreign_key_constraints)
        cross_database: set[sa.ForeignKeyConstraint] = set()
        if own_fks and self.resolved is not None:
            metadata = table_cls.__table__.metadata
            cross_database = {
                entry.constraint
                for entry in unenforced_foreign_keys(
                    tuple(metadata.tables.values()), resolved=self.resolved, physical_schemas=self.physical_schemas
                )
                if entry.reason == "cross_database"
            }
        for fk in own_fks:
            if fk in cross_database:
                self._validate_cross_database_fk(fk, session)
            else:
                self._validate_one_fk(fk, session)

    def _validate_cross_database_fk(
        self,
        fk: sa.ForeignKeyConstraint,
        session: so.Session,
        error: type[ForeignKeyViolationError] = ForeignKeyViolationError,
    ) -> None:
        """Anti-join fk across two connections by comparing both sides in Python.

        Pulls fk's distinct non-null local values from *session*'s own
        connection, then opens the referred table's own connection (via
        self.resolved) to check which of them actually exist there. One
        bulk query per side instead of one query spanning both.

        Raises
        ------
        TypeError
            self.resolved is not a ResolvedCDMDatabase: a cross-database FK
            can only arise when PRIMARY/VOCAB are split across connections,
            which only a CDM-aware resolved can describe.
        ValueError
            The remote role's connection is ephemeral (DuckDB ``:memory:``):
            a freshly built engine would get its own, empty database rather
            than the one already holding the remote data.
        ForeignKeyViolationError
            A local value has no match on the referred connection.
        """
        if not isinstance(self.resolved, ResolvedCDMDatabase):
            raise TypeError(
                f"{fk.name} was classified cross_database, which requires split PRIMARY/VOCAB "
                f"connections, but self.resolved is {type(self.resolved).__name__}, not "
                "ResolvedCDMDatabase."
            )
        local_cols = list(fk.columns)
        remote_cols = [element.column for element in fk.elements]
        not_null = sa.and_(*(col.isnot(None) for col in local_cols))
        local_values = session.execute(sa.select(*local_cols).where(not_null).distinct()).all()
        if not local_values:
            return

        remote_tag = remote_cols[0].table.schema
        remote_role = Role.VOCAB if remote_tag == Role.VOCAB.value else Role.PRIMARY
        remote_connection = self.resolved.connection_for_role(remote_role)
        if is_ephemeral_url(remote_connection.safe_url):
            raise ValueError(
                f"{fk.name} crosses to {remote_connection.name!r}, an ephemeral connection "
                "(DuckDB ':memory:'): a freshly built engine would see an empty, disconnected "
                "database instead of the data already loaded there. Cross-database FK "
                "validation needs a real, file-backed or server-backed remote connection."
            )
        from ..helpers.engines import dispose_engines

        primary_engine, vocab_engine = self.resolved.create_engines(register_claims=False)
        try:
            remote_engine = vocab_engine if remote_tag == Role.VOCAB.value else primary_engine
            with remote_engine.connect() as remote_conn:
                if len(remote_cols) == 1:
                    condition = remote_cols[0].in_(value for (value,) in local_values)
                else:
                    condition = sa.tuple_(*remote_cols).in_(tuple(value) for value in local_values)
                existing = {tuple(row) for row in remote_conn.execute(sa.select(*remote_cols).where(condition))}
        finally:
            dispose_engines(primary_engine, vocab_engine)

        missing = [tuple(value) for value in local_values if tuple(value) not in existing]
        if missing:
            raise error(fk, missing[:5])

    @staticmethod
    def _validate_one_fk(
        fk: sa.ForeignKeyConstraint,
        session: so.Session,
        error: type[ForeignKeyViolationError] = ForeignKeyViolationError,
    ) -> None:
        """Raise if any row of fk's local table has a non-null value with no match in the referred table.

        Parameters
        ----------
        fk : sa.ForeignKeyConstraint
            The FK constraint to check.
        session : so.Session
            Session to run the anti-join query against.
        error
            Exception raised on a violation.

        Raises
        ------
        ForeignKeyViolationError
            If any row's FK column is non-null with no matching row in the
            referenced table (``error`` is raised, a subclass).

        Notes
        -----
        The referred table is always aliased, even for an ordinary
        cross-table FK where it changes nothing. A self-referential FK's
        referred table is the same Table object as the row being checked,
        so an unaliased subquery would compare each row to itself instead
        of correlating to the outer row.
        """
        local_cols = list(fk.columns)
        remote_cols = [element.column for element in fk.elements]
        referred_alias = remote_cols[0].table.alias()
        aliased_remote_cols = [referred_alias.c[col.name] for col in remote_cols]
        matches = sa.and_(*(local_cols[i] == aliased_remote_cols[i] for i in range(len(local_cols))))
        not_null = sa.and_(*(col.isnot(None) for col in local_cols))
        orphan_exists = sa.exists(sa.select(sa.literal(1)).select_from(referred_alias).where(matches))

        orphans = session.execute(
            sa.select(*local_cols).where(not_null, ~orphan_exists).limit(5)
        ).all()
        if orphans:
            raise error(fk, [tuple(row) for row in orphans])

    def create_staging_table(
        self,
        table_cls: Type["FileTableProtocol"],
        session: so.Session,
        *,
        without: Collection[str] = (),
    ) -> None:
        """
        Create the staging table for a table class, replacing an existing one.

        The staging table has the target's columns with their types and
        nullability, and no key, index, default or constraint, plus the row
        key column of the backend. The processor completes every value before
        it is written, so the target's NOT NULL holds in staging.

        Parameters
        ----------
        table_cls
            Table the staging table is for.
        session
            Session whose connection creates the table.
        without
            Names of target columns the staging table leaves out. The merge
            then leaves them out of its insert.
        """
        self.drop_staging_table(table_cls, session)
        columns = [
            sa.Column(c.name, c.type, nullable=c.nullable)
            for c in table_cls.__table__.columns
            if c.computed is None and c.name not in without
        ]
        staging = self._staging_table(table_cls, columns)
        staging.create(bind=session.connection(bind_arguments={"mapper": table_cls}), checkfirst=True)
        session.commit()

    def drop_staging_table(
        self,
        table_cls: Type["FileTableProtocol"],
        session: so.Session,
    ) -> None:
        """Drop a staging table and what belongs to its row key, if they exist."""
        staging = self._staging_table(table_cls, [])
        staging.drop(bind=session.connection(bind_arguments={"mapper": table_cls}), checkfirst=True)

    def _staging_table(self, table_cls: Type["FileTableProtocol"], columns: Sequence[sa.Column[Any]]) -> sa.Table:
        """
        Return the definition of the staging table, which create and drop both use.

        Parameters
        ----------
        table_cls
            Table the staging table is for.
        columns
            The target's columns the staging table holds, none when dropping.

        Returns
        -------
        sqlalchemy.Table
            The columns plus the backend's row key column. The default has no
            row key, for a backend whose tables already carry one.
        """
        return sa.Table(
            self.staging_name_for_table(table_cls.__tablename__),
            sa.MetaData(),
            *columns,
            schema=self.staging_schema,
        )

    def evaluate_default(
        self,
        session: so.Session,
        table_cls: Type["FileTableProtocol"],
        expression: sa.ColumnElement[Any],
        column_type: Any,
    ) -> Any:
        """
        Return the value of a SQL expression default, as a column of the given type stores it.

        The database evaluates the expression and casts the result to the
        column type, as it would for the default of an inserted row.
        """
        select = sa.select(sa.cast(expression, column_type))
        return session.execute(select, bind_arguments={"mapper": table_cls}).scalar_one()

    @abstractmethod
    def insert_batches(
        self,
        ctx: "LoaderContext",
        staging: sa.Table,
        batches: Iterable[pa.Table],
        columns: Sequence[str],
    ) -> None:
        """
        Write typed batches into the load's staging table.

        Each backend uses the fastest bulk interface its engine offers.

        Parameters
        ----------
        ctx
            Context of the load.
        staging
            Staging table that receives the rows.
        batches
            Typed tables, one per batch.
        columns
            Names of the columns the batches carry.

        Notes
        -----
        A backend may commit between batches to bound memory, which also ends
        the caller's open transaction.
        """

    @staticmethod
    def _staging_reference(staging: sa.Table, connection: Connection) -> str:
        """
        Return the quoted, schema-qualified name of the load's staging table.

        The schema is resolved through the connection's ``schema_translate_map``,
        for raw SQL that SQLAlchemy does not translate itself.

        Parameters
        ----------
        staging
            The staging table.
        connection
            Connection the SQL runs on.

        Returns
        -------
        str
            The staging table name as the connection resolves it.
        """
        translate = connection.get_execution_options().get("schema_translate_map") or {}
        schema = translate.get(staging.schema, staging.schema)
        return qualified(connection, staging.name, physical_schema=schema)

    def count_staging(self, ctx: "LoaderContext", staging: sa.Table) -> int:
        """Return the number of rows in the staging table."""
        count = sa.select(sa.func.count()).select_from(staging)
        return ctx.session.execute(count, bind_arguments={"mapper": ctx.tableclass}).scalar_one()

    def deduplicate_staging(
        self,
        ctx: "LoaderContext",
        staging: sa.Table,
        key_columns: Sequence[str],
    ) -> "DedupeResult":
        """
        Keep the first row for each primary key in the staging table.

        The first row is the one with the lowest row key, which follows the
        order of the source file.

        Parameters
        ----------
        ctx
            Context of the load.
        staging
            Staging table to deduplicate.
        key_columns
            Primary key columns that identify a duplicate.

        Returns
        -------
        DedupeResult
            How many rows the staging table held and how many were removed.
        """
        arguments = {"mapper": ctx.tableclass}
        row_key = self._pagination_column(ctx.tableclass, staging)
        ranked = sa.select(
            row_key.label("row_key"),
            sa.func.row_number()
            .over(partition_by=[staging.c[name] for name in key_columns], order_by=row_key)
            .label("position"),
        ).subquery()
        duplicates = sa.select(ranked.c.row_key).where(ranked.c.position > 1)
        before = self.count_staging(ctx, staging)
        ctx.session.execute(sa.delete(staging).where(row_key.in_(duplicates)), bind_arguments=arguments)
        after = self.count_staging(ctx, staging)
        return DedupeResult(tuple(key_columns), before, before - after)

    @staticmethod
    @abstractmethod
    def _normalize_fk_check_state(previous_state: str | int) -> str | int:
        """Validate and normalise a previously-returned FK state before interpolating into SQL.

        Each backend accepts a different type (SQLite: int, Postgres: str) and must
        implement this to guard restore_fk_check() against invalid or injected values.
        """

    @abstractmethod
    def disable_fk_check(self, session: so.Session, *, mapper: Any | None = None) -> str | int:
        """Disable FK checks and return the previous backend-specific state."""

    @abstractmethod
    def enable_fk_check(self, session: so.Session, *, mapper: Any | None = None) -> str | int:
        """Explicitly enable FK checks and return the previous backend-specific state."""

    @abstractmethod
    def restore_fk_check(
        self,
        session: so.Session,
        previous_state: str | int,
        *,
        mapper: Any | None = None,
    ) -> None:
        """Restore FK checks to a previously returned backend-specific state."""

    def check_replace_feasible(
        self,
        table_cls: Type["FileTableProtocol"],
        pk_cols: list[str],
        session: so.Session,
    ) -> None:
        """Raise if merge_strategy='replace' can't load table_cls on this backend.

        Default: always feasible. Only a backend that rejects certain
        in-place updates on a currently-referenced row (DuckDB) overrides
        this, so callers can preflight every table a replace load will
        touch before loading any of them. session is only used as read-only
        to check whether target or a referencing table already has rows
        """

    @abstractmethod
    def merge_replace(
        self,
        table_cls: Type["FileTableProtocol"],
        session: so.Session,
        pk_cols: list[str],
        *,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ) -> None:
        """Merge staging rows by replacing matching target rows first.

        validate_deferred_fks
            Forwarded to :meth:`_commit_batch_if_needed` for each
            intermediate commit, so a batch that violates a deferred FK is
            caught before it commits instead of only after the whole merge.
        """

    @abstractmethod
    def merge_upsert(
        self,
        table_cls: Type["FileTableProtocol"],
        session: so.Session,
        pk_cols: list[str],
        *,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ) -> None:
        """Merge staging rows using backend-specific upsert semantics.

        validate_deferred_fks
            See :meth:`merge_replace`.
        """

    @abstractmethod
    def merge_insert(
        self,
        table_cls: Type["FileTableProtocol"],
        session: so.Session,
        *,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ) -> None:
        """Insert all staging rows into the target table.

        validate_deferred_fks
            See :meth:`merge_replace`.
        """

    def _pagination_column(
        self,
        table_cls: Type["FileTableProtocol"],
        staging: sa.Table,
    ) -> sa.ColumnElement[int]:
        """Column this backend orders a staging-table scan by, for pagination.

        Defaults to the explicit _rownum column every staging table carries.
        Override for a backend with a cheaper, already-indexed alternative
        (e.g. SQLite's implicit rowid).
        """
        return staging.c._rownum

    def _ensure_pagination_index(
        self,
        table_cls: Type["FileTableProtocol"],
        staging: sa.Table,
        session: so.Session,
    ) -> None:
        """Index _pagination_column() so paginated batches can seek it efficiently.

        Overridden as a no-op by a backend whose _pagination_column() is
        already indexed (e.g. SQLite's rowid).
        """
        staging_name = self.staging_name_for_table(table_cls.__tablename__)
        idx = sa.Index(f"{staging_name}_rownum_idx", staging.c._rownum)
        idx.create(bind=session.connection(bind_arguments={"mapper": table_cls}), checkfirst=True)
        session.commit()

    @property
    @abstractmethod
    def _commits_between_pagination_batches(self) -> bool:
        """Whether _paginate_batches()'s caller should commit after each batch."""

    def _paginate_batches(
        self,
        table_cls: Type["FileTableProtocol"],
        staging: sa.Table,
        session: so.Session,
        merge_batch_size: int | None,
    ) -> Iterator[sa.ColumnElement[bool] | None]:
        """Yield one window predicate per batch for a full scan of staging.

        Yields
        ------
        sa.ColumnElement[bool] or None
            A predicate to filter the staging table to a single batch of rows.
            None if merge_batch_size is None or the staging table is smaller than 
            merge_batch_size.
        """
        if merge_batch_size is not None:
            total = session.execute(sa.select(sa.func.count()).select_from(staging)).scalar_one()
        if merge_batch_size is None or total <= merge_batch_size:
            yield None
            return

        column = self._pagination_column(table_cls, staging)
        self._ensure_pagination_index(table_cls, staging, session)
        bound = session.execute(
            sa.select(sa.func.coalesce(sa.func.max(column), 0)).select_from(staging)
        ).scalar_one()
        start = 0
        while start < bound:
            end = start + merge_batch_size
            yield sa.and_(column > start, column <= end)
            start = end

    def _commit_batch_if_needed(
        self,
        table_cls: Type["FileTableProtocol"],
        session: so.Session,
        batch_range: sa.ColumnElement[bool] | None,
        *,
        validate_deferred_fks: bool = True,
    ) -> None:
        """Validate deferred FKs and commit after one pagination batch, 
        if this backend commits between batches.

        Called from every merge_*() method after each batch of rows is processed. 
        Instead of committing directly, a batch that violates a deferred FK is caught
        before the next batch starts. Without this, an earlier batch's commit cannot
        be undone once a later batch's load_file()-level validation fails.

        No-op when there is nothing to validate to not add cost. This is true for every 
        backend but DuckDB, and for DuckDB whenever table_cls has no deferred/
        unenforced FK at all.
        """
        if batch_range is None or not self._commits_between_pagination_batches:
            return
        if validate_deferred_fks:
            self.validate_deferred_fks(table_cls, session)
        session.commit()

    def _self_referential_deferred_fks(
        self, table_cls: Type["FileTableProtocol"]
    ) -> tuple[sa.ForeignKeyConstraint, ...]:
        """Deferred FKs on table_cls that point back at table_cls itself.

        A row's match for one of these can arrive in a later pagination
        batch of the same load, since batching orders by insertion
        position rather than by dependency. A backend with any of these
        must override :meth:`_paginate_batches` to yield batches in
        dependency order instead.
        """
        target = table_cls.__table__
        return tuple(fk for fk in self.deferred_fk_constraints(table_cls) if fk.referred_table is target)

    def merge_context(
        self,
        table_cls: Type["FileTableProtocol"],
        session: so.Session,
    ) -> AbstractContextManager[None]:
        """Disable backend FK checks for a table's merge and restore them afterward.

        A backend without supports_fk_toggle has no state to disable in the
        first place, so this skips the attempt entirely rather than raising;
        such a backend's own merge methods must avoid operations (like
        deleting a referenced row) that would need the toggle to be safe.
        """
        return self.bulk_load_context(
            session, mapper=table_cls, disable_fk=self.capabilities.supports_fk_toggle, no_autoflush=False
        )

    @contextmanager
    def bulk_load_context(
        self,
        session: so.Session,
        *,
        mapper: Any | None = None,
        disable_fk: bool = True,
        no_autoflush: bool = True,
    ):
        """
        Generic bulk-load context that defers FK semantics to the backend.
        """
        previous_fk_state: str | int | None = None
        try:
            if disable_fk:
                self._require_capability("supports_fk_toggle", "foreign key toggling")
                raw_state = (
                    self.disable_fk_check(session)
                    if mapper is None
                    else self.disable_fk_check(session, mapper=mapper)
                )
                previous_fk_state = self._normalize_fk_check_state(raw_state)

            if no_autoflush:
                with session.no_autoflush:
                    yield
            else:
                yield

        except Exception:
            session.rollback()
            raise

        finally:
            if previous_fk_state is not None:
                if mapper is None:
                    self.restore_fk_check(session, previous_fk_state)
                else:
                    self.restore_fk_check(session, previous_fk_state, mapper=mapper)

    @requires_capability("supports_materialized_views", "materialized views")
    def create_materialized_view(
        self,
        bind: "Engine | Connection",
        name: str,
        selectable: sa.sql.Select[Any],
        *,
        schema: str | None = None,
        with_data: bool = True,
        if_not_exists: bool = True,
    ) -> None:
        """Create a materialized view for the supplied selectable.

        *schema* is the view's already-resolved physical schema (or None for
        the connection's own default); callers resolve which
        schema_translate_map key to use before calling.
        """
        raise NotImplementedError(
            f"Backend '{self.name}' has not implemented create_materialized_view()"
        )

    @requires_capability("supports_materialized_views", "materialized views")
    def refresh_materialized_view(
        self,
        bind: "Engine | Connection",
        name: str,
        *,
        schema: str | None = None,
        concurrently: bool = False,
        declared_indexes: tuple["MaterializedViewIndex", ...] = (),
    ) -> None:
        """Refresh a materialized view.

        *schema* is the view's already-resolved physical schema, matching
        ``create_materialized_view``.

        ``declared_indexes`` lets supporting backends validate a concurrent
        refresh request without defining a second catalog-based eligibility
        rule. Other backends may ignore it.
        """
        raise NotImplementedError(
            f"Backend '{self.name}' has not implemented refresh_materialized_view()"
        )

    @requires_capability("supports_materialized_views", "materialized views")
    def drop_materialized_view(
        self,
        bind: "Engine | Connection",
        name: str,
        *,
        schema: str | None = None,
        if_exists: bool = True,
        cascade: bool = False,
    ) -> None:
        """Drop a materialized view.

        *schema* is the view's already-resolved physical schema, matching
        ``create_materialized_view``. This is deliberately non-abstract: the
        default implementation requires the capability flag and then raises
        ``NotImplementedError``. Older third-party backend subclasses need
        no override to receive a clear error when they do not support this
        operation.
        """
        raise NotImplementedError(
            f"Backend '{self.name}' has not implemented drop_materialized_view()"
        )

    @requires_capability("supports_materialized_views", "materialized views")
    def create_materialized_view_index(
        self,
        bind: "Engine | Connection",
        name: str,
        index: "MaterializedViewIndex",
        *,
        schema: str | None = None,
        if_not_exists: bool = True,
    ) -> None:
        """Create an index on a materialized view.

        *schema* is the view's already-resolved physical schema, matching
        ``create_materialized_view``. This is deliberately non-abstract for
        the same compatibility reason as :meth:`drop_materialized_view`.
        """
        raise NotImplementedError(
            f"Backend '{self.name}' has not implemented "
            "create_materialized_view_index()"
        )
