from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Type, cast

import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import (
    ResolvedCDMDatabase,
    Role,
    UnenforcedForeignKey,
    is_ephemeral_url,
    physical_schema_of,
    qualified,
    unenforced_foreign_keys,
)
from sqlalchemy.schema import CreateSequence, DropSequence, DropTable
from sqlalchemy.sql.compiler import IdentifierPreparer

from ..helpers.engines import dispose_engines
from .base import BackendCapabilities, DatabaseBackend, DeferredForeignKeyViolationError, Dialect

if TYPE_CHECKING:
    from ..tables.typing import CSVTableProtocol


class DuckDBBackend(DatabaseBackend):
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

    @property
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            supports_fast_load=False,
            supports_unlogged_staging=False,
            supports_fk_toggle=False,
            supports_materialized_views=False,
            supports_replace_with_kept_indexes=False,
        )

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
                "enforcement. Pass it to load_csv(...) or resolve_backend(...)."
            )
        return unenforced_foreign_keys(
            tables, resolved=self.resolved, physical_schemas=self.physical_schemas
        )

    def deferred_fk_constraints(
        self,
        table_cls: Type["CSVTableProtocol"],
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

    def validate_deferred_fks(self, table_cls: Type["CSVTableProtocol"], session: so.Session) -> None:
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
                self._validate_cross_database_fk(entry.constraint, session)
            else:
                self._validate_one_deferred_fk(entry.constraint, session)

    def _validate_cross_database_fk(self, fk: sa.ForeignKeyConstraint, session: so.Session) -> None:
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
        DeferredForeignKeyViolationError
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
            raise DeferredForeignKeyViolationError(fk, missing[:5])

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

    def _staging_rownum_sequence_ref(self, tablename: str) -> str:
        """Fully schema-qualified, quoted name of tablename's staging _rownum sequence.

        DuckDB can't ALTER TABLE ADD a GENERATED ALWAYS AS IDENTITY column
        after creation, so pagination uses a sequence-backed DEFAULT
        instead for a stable, monotonic key.
        """
        seq_name = f"{self.staging_name_for_table(tablename)}_rownum_seq"
        return qualified(self.identifier_preparer, seq_name, physical_schema=self.staging_schema)

    def create_staging_table(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
    ) -> None:
        bind_arguments = {"mapper": table_cls}
        staging_name = self.staging_name_for_table(table_cls.__tablename__)
        staging_ref = self.qualified_staging_name(table_cls.__tablename__)
        seq_ref = self._staging_rownum_sequence_ref(table_cls.__tablename__)
        sequence = sa.Sequence(f"{staging_name}_rownum_seq", schema=self.staging_schema)

        session.execute(
            DropTable(sa.Table(staging_name, sa.MetaData(), schema=self.staging_schema), if_exists=True),
            bind_arguments=bind_arguments,
        )
        session.execute(DropSequence(sequence, if_exists=True), bind_arguments=bind_arguments)

        metadata = sa.MetaData()
        staging_columns = [
            sa.Column(col.name, col.type, nullable=True)
            for col in table_cls.__table__.columns
        ]
        staging_table = sa.Table(staging_name, metadata, *staging_columns, schema=self.staging_schema)
        metadata.create_all(
            bind=session.connection(bind_arguments=bind_arguments), tables=[staging_table]
        )

        session.execute(CreateSequence(sequence), bind_arguments=bind_arguments)
        # ALTER TABLE ADD COLUMN has no Core DDL construct, so this stays raw.
        session.execute(
            sa.text(f"ALTER TABLE {staging_ref} ADD COLUMN _rownum BIGINT DEFAULT nextval('{seq_ref}');"),
            bind_arguments=bind_arguments,
        )
        session.commit()

    def drop_staging_table(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
    ) -> None:
        bind_arguments = {"mapper": table_cls}
        staging_name = self.staging_name_for_table(table_cls.__tablename__)
        session.execute(
            DropTable(sa.Table(staging_name, sa.MetaData(), schema=self.staging_schema), if_exists=True),
            bind_arguments=bind_arguments,
        )
        session.execute(
            DropSequence(
                sa.Sequence(f"{staging_name}_rownum_seq", schema=self.staging_schema), if_exists=True
            ),
            bind_arguments=bind_arguments,
        )

    @staticmethod
    def _normalize_fk_check_state(previous_state: str | int) -> str | int:
        raise NotImplementedError(
            "DuckDB has no foreign-key-toggle mechanism; there is no state to normalize."
        )

    def disable_fk_check(self, session: so.Session, *, mapper: Any | None = None) -> str | int:
        raise NotImplementedError(
            "DuckDB has no foreign-key-toggle mechanism (no PRAGMA/SET equivalent exists)."
        )

    def enable_fk_check(self, session: so.Session, *, mapper: Any | None = None) -> str | int:
        raise NotImplementedError(
            "DuckDB has no foreign-key-toggle mechanism (no PRAGMA/SET equivalent exists)."
        )

    def restore_fk_check(
        self,
        session: so.Session,
        previous_state: str | int,
        *,
        mapper: Any | None = None,
    ) -> None:
        raise NotImplementedError(
            "DuckDB has no foreign-key-toggle mechanism (no PRAGMA/SET equivalent exists)."
        )

    def _pagination_column(
        self, table_cls: type["CSVTableProtocol"], staging: sa.Table
    ) -> sa.ColumnElement[int]:
        return staging.c._rownum

    def _ensure_pagination_index(
        self, table_cls: type["CSVTableProtocol"], staging: sa.Table, session: so.Session
    ) -> None:
        staging_name = self.staging_name_for_table(table_cls.__tablename__)
        idx = sa.Index(f"{staging_name}_rownum_idx", staging.c._rownum)
        idx.create(bind=session.connection(bind_arguments={"mapper": table_cls}), checkfirst=True)
        session.commit()

    def _paginate_batches(
        self,
        table_cls: type["CSVTableProtocol"],
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
        table_cls: type["CSVTableProtocol"],
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

    def check_replace_feasible(
        self,
        table_cls: type["CSVTableProtocol"],
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
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
        pk_cols: list[str],
        *,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ) -> None:
        """Update matching target rows from staging in place, then drop them from staging.

        DuckDB can't delete a still-referenced row, so replace is an UPDATE
        rather than delete-then-reinsert. Requires index_strategy to have
        dropped indexes first; capabilities.supports_replace_with_kept_indexes
        is False for this reason.

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
        staging = table_cls.get_staging_table(session, staging_schema_tag=self.staging_schema_tag)
        update_cols = [c for c in self._insertable_column_names(table_cls) if c not in pk_cols]
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
            matched = sa.select(*(target.c[c] for c in pk_cols)).where(pk_join)
            if len(pk_cols) == 1:
                condition: sa.ColumnElement[bool] = staging.c[pk_cols[0]].in_(matched)
            else:
                condition = sa.tuple_(*(staging.c[c] for c in pk_cols)).in_(matched)
            stmt = sa.delete(staging).where(condition)
            if extra is not None:
                stmt = stmt.where(extra)
            session.execute(stmt)

        if not update_cols:
            # No non-pk columns to update: matching rows are already identical, just drop them.
            _drop_matched_from_staging()
            return

        self_referential_fks = self._self_referential_deferred_fks(table_cls)
        if self_referential_fks:
            insertable_cols = self._insertable_column_names(table_cls)
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
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
        pk_cols: list[str],
        *,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ) -> None:
        import duckdb_sqlalchemy

        self._validate_merge_batch_size(merge_batch_size)
        target = table_cls.__table__
        staging = table_cls.get_staging_table(session, staging_schema_tag=self.staging_schema_tag)
        insertable_cols = self._insertable_column_names(table_cls)

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
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
        *,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ) -> None:
        self._validate_merge_batch_size(merge_batch_size)
        target = table_cls.__table__
        staging = table_cls.get_staging_table(session, staging_schema_tag=self.staging_schema_tag)
        insertable_cols = self._insertable_column_names(table_cls)
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
