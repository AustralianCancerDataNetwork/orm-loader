from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Type, cast

import pyarrow as pa
import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import (
    UnenforcedForeignKey,
    physical_schema_of,
    unenforced_foreign_keys,
)
from sqlalchemy.sql.compiler import IdentifierPreparer

from .base import DatabaseBackend, DeferredForeignKeyViolationError, Dialect

if TYPE_CHECKING:
    from ..loaders.loader import LoaderContext
    from ..tables.typing import FileTableProtocol


class DuckDBBackend(DatabaseBackend):
    supports_replace_with_kept_indexes = False

    @staticmethod
    def staging_name_for_table(tablename: str) -> str:
        return f"_staging_{tablename}"

    @property
    def name(self) -> str:
        return Dialect.DUCKDB

    @property
    def dialect(self) -> Dialect:
        return Dialect.DUCKDB

    @property
    def identifier_preparer(self) -> IdentifierPreparer:
        import duckdb_sqlalchemy

        return duckdb_sqlalchemy.Dialect().identifier_preparer

    # Batches per commit of the staging table. DuckDB keeps uncommitted rows in memory, so this bounds
    # the memory of a load.
    STAGING_COMMIT_EVERY: ClassVar[int] = 5

    def _unenforced_constraints(
        self, tables: tuple[sa.Table, ...]
    ) -> tuple[UnenforcedForeignKey, ...]:
        """Every FK in *tables* DuckDB will not enforce, including reason: 
        - omitted (cross-database/cross-schema) or 
        - created but never added (cyclic/``use_alter=True``).

        Delegates to oa-configurator's :func:`unenforced_foreign_keys`, the
        same classification DDL creation itself uses.

        Raises
        ------
        ValueError
            ``self.resolved`` was never given to ``resolve_backend()``.
        """
        if self.resolved is None:
            raise ValueError(
                "DuckDBBackend needs resolved=<ResolvedDatabase> to classify foreign-key "
                "enforcement. Pass it to load_file(...) or resolve_backend(...)."
            )
        return unenforced_foreign_keys(
            tables, resolved=self.resolved, physical_schemas=self.physical_schemas
        )

    def deferred_fk_constraints(
        self,
        table_cls: Type["FileTableProtocol"],
    ) -> tuple[sa.ForeignKeyConstraint, ...]:
        """FK constraints on *table_cls* this backend's database will not enforce:
        - omitted (cross-database/cross-schema), or
        - created but never added (cyclic/``use_alter=True``).
        See :meth:`validate_deferred_fks`.
        """
        own_fks = tuple(table_cls.__table__.foreign_key_constraints)
        if not own_fks:
            return ()
        metadata = table_cls.__table__.metadata
        unenforced = {entry.constraint for entry in self._unenforced_constraints(tuple(metadata.tables.values()))}
        return tuple(fk for fk in own_fks if fk in unenforced)

    def validate_deferred_fks(self, table_cls: Type["FileTableProtocol"], session: so.Session) -> None:
        """Validate every deferred FK on table_cls, including a cross-database one.

        A cross-schema or cyclic deferred FK points to a table in the same
        database, so one query can check both tables directly. A
        cross-database FK points to a table in a different database: a
        query touching both would raise CrossDatabaseStatementError no
        matter what the data actually is, so _validate_cross_database_fk
        runs one query per database instead.
        """
        own_fks = tuple(table_cls.__table__.foreign_key_constraints)
        if not own_fks:
            return
        metadata = table_cls.__table__.metadata
        for entry in self._unenforced_constraints(tuple(metadata.tables.values())):
            if entry.constraint not in own_fks:
                continue
            if entry.reason == "cross_database":
                self._validate_cross_database_fk(entry.constraint, session, DeferredForeignKeyViolationError)
            else:
                self._validate_one_fk(entry.constraint, session, DeferredForeignKeyViolationError)

    def validate_foreign_keys(self, table_cls: Type["FileTableProtocol"], session: so.Session) -> None:
        """Raise if any row in *table_cls* references a row that does not exist.

        DuckDB enforces the foreign keys it creates, so only the ones it
        never enforces need an anti-join, which is
        :meth:`validate_deferred_fks`.
        """
        self.validate_deferred_fks(table_cls, session)

    def _tables_referencing(self, target: sa.Table) -> set[sa.Table]:
        """Other tables with an enforced FK pointing at target.

        Excludes an omitted/deferred FK: DuckDB has no real constraint
        there, so it must not count toward check_replace_feasible()'s
        preflight.
        """
        tables = tuple(target.metadata.tables.values())
        incoming = tuple(
            fk for other in tables if other is not target for fk in other.foreign_key_constraints
            if fk.referred_table is target
        )
        if not incoming:
            return set()
        unenforced = {entry.constraint for entry in self._unenforced_constraints(tables)}
        return {fk.table for fk in incoming if fk not in unenforced}

    def _is_referenced_by_existing_rows(self, target: sa.Table, session: so.Session) -> bool:
        """True if some table referencing target has any rows.

        Table-level existence check. A referencing table may not exist yet,
        since target's shared metadata spans every CDM table.
        """
        bind = session.get_bind()
        inspector = sa.inspect(bind)
        return any(
            inspector.has_table(other.name, schema=physical_schema_of(bind, schema_tag=other.schema))
            and session.execute(sa.select(sa.literal(1)).select_from(other).limit(1)).first() is not None
            for other in self._tables_referencing(target)
        )

    def _staging_table(self, table_cls: type["FileTableProtocol"], columns: Sequence[sa.Column[Any]]) -> sa.Table:
        """Number the rows with a sequence, which DuckDB creates and drops with the table."""
        name = self.staging_name_for_table(table_cls.__tablename__)
        sequence = sa.Sequence(f"{name}_rownum_seq", schema=self.staging_schema)
        row_key = sa.Column("_rownum", sa.BigInteger, sequence, server_default=sequence.next_value())
        return sa.Table(name, sa.MetaData(), *columns, row_key, schema=self.staging_schema)

    def _paginate_batches(
        self,
        table_cls: type["FileTableProtocol"],
        staging: sa.Table,
        session: so.Session,
        merge_batch_size: int | None,
    ) -> Iterator[sa.ColumnElement[bool] | None]:
        """Yield batches in dependency order when a self-referential deferred FK exists.

        Falls back to the base class's ordinary position-based pagination
        for every other table. See :meth:`_self_referential_batches`.
        """
        self_referential_fks = self._self_referential_deferred_fks(table_cls)
        if not self_referential_fks:
            yield from super()._paginate_batches(table_cls, staging, session, merge_batch_size)
            return
        yield from self._self_referential_batches(
            table_cls, staging, session, merge_batch_size, self_referential_fks
        )

    def _self_referential_batches(
        self,
        table_cls: type["FileTableProtocol"],
        staging: sa.Table,
        session: so.Session,
        merge_batch_size: int | None,
        self_referential_fks: tuple[sa.ForeignKeyConstraint, ...],
    ) -> Iterator[sa.ColumnElement[bool] | None]:
        """Yield staging batches in the order a self-referential deferred FK needs.

        Each batch is the staging rows whose self-referential FK
        value(s) are already satisfied (NULL, or matching a row already
        committed to target). The caller commits and cleans up staging
        for each batch before the next is requested; this generator
        never deletes staging itself, so that cleanup stays in the same
        transaction as the batch's own insert.

        Once no further batch can make progress, everything left in
        staging is yielded as one final, unconditional batch, deferring
        to the caller's insert/upsert statement and its own
        validate_deferred_fks() check rather than raising here.

        When merge_batch_size is None, yields the entire staging set as
        a single unfiltered batch with no readiness computation.
        """
        if merge_batch_size is None:
            yield None
            return

        target = table_cls.__table__
        order_column = self._pagination_column(table_cls, staging)
        self._ensure_pagination_index(table_cls, staging, session)

        per_fk = []
        for fk in self_referential_fks:
            local_cols: list[sa.ColumnElement[Any]] = [staging.c[col.name] for col in fk.columns]
            remote_cols = [cast(sa.ColumnElement[Any], element.column) for element in fk.elements]
            not_null = sa.and_(*(col.isnot(None) for col in local_cols))
            has_match = sa.exists(
                sa.select(sa.literal(1)).select_from(target).where(
                    sa.and_(*(local_cols[i] == remote_cols[i] for i in range(len(remote_cols))))
                )
            )
            per_fk.append((not_null, has_match))
        ready = sa.and_(*(sa.or_(sa.not_(not_null), has_match) for not_null, has_match in per_fk))

        while True:
            query = sa.select(order_column).where(ready).order_by(order_column).limit(merge_batch_size)
            keys = session.execute(query).scalars().all()
            if keys:
                chunk: sa.ColumnElement[bool] = order_column.in_(keys)
            else:
                remaining = session.execute(sa.select(sa.func.count()).select_from(staging)).scalar_one()
                if not remaining:
                    return
                chunk = sa.true()
            yield chunk

    @property
    def _commits_between_pagination_batches(self) -> bool:
        """True: bounds uncommitted work on a large table. Safe because
        merge_replace updates rows in place rather than delete-then-reinsert,
        so a commit between batches never leaves a row removed without its
        replacement.
        """
        return True

    def insert_batches(
        self,
        ctx: "LoaderContext",
        staging: sa.Table,
        batches: Iterable[pa.Table],
        columns: Sequence[str],
    ) -> None:
        """Insert the batches into the staging table straight from Arrow.

        Notes
        -----
        Each batch is registered as a temporary view over the Arrow memory and
        copied with ``INSERT ... SELECT``, which is DuckDB's fastest bulk path.
        The staging table is committed every ``STAGING_COMMIT_EVERY`` batches,
        because DuckDB keeps uncommitted rows in memory and one transaction
        would hold the whole file. The commit also ends the caller's open
        transaction.
        """
        batch = sa.table("_orm_loader_batch", *(sa.column(c) for c in columns))
        insert = staging.insert().from_select(list(columns), sa.select(*batch.c))
        bind_arguments = {"mapper": ctx.tableclass}
        for index, table in enumerate(batches, start=1):
            native = ctx.session.connection(bind_arguments=bind_arguments).connection
            native.register("_orm_loader_batch", table)
            try:
                ctx.session.execute(insert, bind_arguments=bind_arguments)
            finally:
                native.unregister("_orm_loader_batch")
            if index % self.STAGING_COMMIT_EVERY == 0:
                ctx.session.commit()

    def check_replace_feasible(
        self,
        table_cls: type["FileTableProtocol"],
        pk_cols: list[str],
        session: so.Session,
    ) -> None:
        """Raise if merge_strategy='replace' can't update table_cls's own FK columns.

        Call before any replace load touches table_cls, so a later table
        failing this check doesn't leave earlier tables already replaced.

        Raises
        ------
        ValueError
            Target has rows, some table referencing it also has rows, and
            target owns an enforced FK on a column being updated: DuckDB
            rejects a SET on an FK-constrained column of a referenced row.
        """
        target = table_cls.__table__
        update_cols = [c for c in self._insertable_column_names(table_cls) if c not in pk_cols]
        deferred_cols = {col.name for fk in self.deferred_fk_constraints(table_cls) for col in fk.columns}
        enforced_fk_cols = [
            c for c in update_cols
            if c in target.c and target.c[c].foreign_keys and c not in deferred_cols
        ]
        if not enforced_fk_cols:
            return
        if not table_cls._target_has_rows(session=session):
            return
        if not self._is_referenced_by_existing_rows(target, session):
            return
        raise ValueError(
            f"merge_strategy='replace' can't update {target.name}.{enforced_fk_cols} on DuckDB: "
            f"{target.name} is referenced by another table's FK, and DuckDB rejects any SET "
            "on an FK-constrained column of a currently-referenced row. Use "
            "--merge-strategy upsert, or reload into an empty database."
        )

    def merge_replace(
        self,
        table_cls: type["FileTableProtocol"],
        session: so.Session,
        pk_cols: list[str],
        staging: sa.Table,
        *,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ) -> None:
        """Update matching target rows from staging in place, then drop them from staging.

        DuckDB can't delete a still-referenced row, so replace is an UPDATE
        rather than delete-then-reinsert. Requires index_strategy to have
        dropped indexes first; supports_replace_with_kept_indexes is False
        for this reason.

        A self-referential FK can point an existing row at one this load
        hasn't inserted yet. Doing replace then insert as separate phases
        would validate that before the target exists, so both happen
        together here, batch by batch (see _paginate_batches()), draining
        staging. The merge_insert() call that follows has nothing left to
        do.

        Raises
        ------
        ValueError
            See check_replace_feasible().
        """
        self._validate_merge_batch_size(merge_batch_size)
        self.check_replace_feasible(table_cls, pk_cols, session)
        target = table_cls.__table__
        update_cols = [c for c in self._insertable_column_names(table_cls, staging) if c not in pk_cols]
        pk_join = sa.and_(*(target.c[c] == staging.c[c] for c in pk_cols))

        def _batch_keys(extra: sa.ColumnElement[bool] | None = None) -> sa.Select[Any]:
            keys = sa.select(*(staging.c[c] for c in pk_cols))
            if extra is not None:
                keys = keys.where(extra)
            return keys

        def _pk_in(keys: sa.Select[Any]) -> sa.ColumnElement[bool]:
            if len(pk_cols) == 1:
                return target.c[pk_cols[0]].in_(keys)
            return sa.tuple_(*(target.c[c] for c in pk_cols)).in_(keys)

        def _update(extra: sa.ColumnElement[bool] | None = None) -> None:
            stmt = sa.update(target).values({c: staging.c[c] for c in update_cols}).where(pk_join)
            if extra is not None:
                stmt = stmt.where(_pk_in(_batch_keys(extra)))
            session.execute(stmt)

        def _drop_matched_from_staging(extra: sa.ColumnElement[bool] | None = None) -> None:
            stmt = sa.delete(staging).where(sa.exists().where(pk_join))
            if extra is not None:
                stmt = stmt.where(extra)
            session.execute(stmt)

        if not update_cols:
            # No non-pk columns to update: matching rows are already identical, just drop them.
            _drop_matched_from_staging()
            return

        self_referential_fks = self._self_referential_deferred_fks(table_cls)
        if self_referential_fks:
            insertable_cols = self._insertable_column_names(table_cls, staging)
            non_paginated_select = sa.select(*(staging.c[c] for c in insertable_cols))

            def _insert(select_: Any) -> sa.Insert:
                return sa.insert(target).from_select(insertable_cols, select_)

            def _process_batch(batch_range: sa.ColumnElement[bool] | None) -> None:
                # batch_range's rows already in target are updated; what's
                # left in staging afterward is new and gets inserted here.
                _update(batch_range)
                _drop_matched_from_staging(batch_range)
                select_ = non_paginated_select if batch_range is None else non_paginated_select.where(batch_range)
                session.execute(_insert(select_))
                # Runs even when batch_range is None: merge_insert() runs
                # next and needs staging fully drained.
                delete_stmt = sa.delete(staging)
                if batch_range is not None:
                    delete_stmt = delete_stmt.where(batch_range)
                session.execute(delete_stmt)
        else:

            def _process_batch(batch_range: sa.ColumnElement[bool] | None) -> None:
                _update(batch_range)
                _drop_matched_from_staging(batch_range)

        for batch_range in self._paginate_batches(table_cls, staging, session, merge_batch_size):
            _process_batch(batch_range)
            self._commit_batch_if_needed(
                table_cls, session, batch_range, validate_deferred_fks=validate_deferred_fks
            )

    def merge_upsert(
        self,
        table_cls: type["FileTableProtocol"],
        session: so.Session,
        pk_cols: list[str],
        staging: sa.Table,
        *,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ) -> None:
        import duckdb_sqlalchemy

        self._validate_merge_batch_size(merge_batch_size)
        target = table_cls.__table__
        insertable_cols = self._insertable_column_names(table_cls, staging)

        def _upsert(select_: Any) -> sa.Insert:
            return (
                duckdb_sqlalchemy.insert(target)
                .from_select(insertable_cols, select_)
                .on_conflict_do_nothing(index_elements=pk_cols)
            )

        non_paginated_select = sa.select(*(staging.c[c] for c in insertable_cols))
        self_referential = bool(self._self_referential_deferred_fks(table_cls))

        for batch_range in self._paginate_batches(table_cls, staging, session, merge_batch_size):
            select_ = non_paginated_select if batch_range is None else non_paginated_select.where(batch_range)
            session.execute(_upsert(select_))
            if self_referential and batch_range is not None:
                # Must be removed from staging in this same transaction,
                # or the next readiness query would select it again.
                session.execute(sa.delete(staging).where(batch_range))
            self._commit_batch_if_needed(
                table_cls, session, batch_range, validate_deferred_fks=validate_deferred_fks
            )

    def merge_insert(
        self,
        table_cls: type["FileTableProtocol"],
        session: so.Session,
        staging: sa.Table,
        *,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ) -> None:
        self._validate_merge_batch_size(merge_batch_size)
        target = table_cls.__table__
        insertable_cols = self._insertable_column_names(table_cls, staging)
        non_paginated_select = sa.select(*(staging.c[c] for c in insertable_cols))

        def _insert(select_: Any) -> sa.Insert:
            return sa.insert(target).from_select(insertable_cols, select_)

        self_referential = bool(self._self_referential_deferred_fks(table_cls))

        for batch_range in self._paginate_batches(table_cls, staging, session, merge_batch_size):
            select_ = non_paginated_select if batch_range is None else non_paginated_select.where(batch_range)
            session.execute(_insert(select_))
            if self_referential and batch_range is not None:
                # Must be removed from staging in this same transaction,
                # or the next readiness query would select it again.
                session.execute(sa.delete(staging).where(batch_range))
            self._commit_batch_if_needed(
                table_cls, session, batch_range, validate_deferred_fks=validate_deferred_fks
            )
