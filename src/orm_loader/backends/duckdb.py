from __future__ import annotations

from typing import TYPE_CHECKING, Any, Type

import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import qualified
from sqlalchemy.schema import CreateSequence, DropSequence, DropTable
from sqlalchemy.sql.compiler import IdentifierPreparer

from .base import BackendCapabilities, DatabaseBackend, Dialect

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

    def deferred_fk_constraints(
        self,
        table_cls: Type["CSVTableProtocol"],
    ) -> tuple[sa.ForeignKeyConstraint, ...]:
        """FK constraints on *table_cls* excluded from its CREATE TABLE and never
        added afterward: DuckDB can't execute the ALTER TABLE ADD CONSTRAINT that
        would otherwise add them (oa-configurator's _skip_deferred_fk_on_duckdb no-ops it).

        sort_tables_and_constraints needs every table in the schema to tell
        which FKs are genuinely deferred (cyclic), so this runs against
        table_cls's whole shared metadata, not table_cls alone.

        Only reads the ``tbl is None`` bucket of sort_tables_and_constraints's
        output. A standalone self-referential FK (not part of a cross-table
        cycle) is still deferred by SQLAlchemy either way, but lands in its
        *own* table's bucket unless the column declares ``use_alter=True``.
        Without that, this method can't see it, and it will insert without
        error instead of being caught by validate_deferred_fks(). Model
        authors: mark a self-referential FK ``use_alter=True`` explicitly if
        it needs anti-join validation on DuckDB.
        """
        metadata = table_cls.__table__.metadata
        entries = sa.schema.sort_tables_and_constraints(metadata.tables.values())
        deferred = next(fks for tbl, fks in entries if tbl is None)
        return tuple(fk for fk in deferred if fk.table is table_cls.__table__)

    @staticmethod
    def _is_referenced_by_other_table(target: sa.Table) -> bool:
        """True if some other table in target's own metadata has an FK pointing at target."""
        return any(
            fk.referred_table is target
            for other in target.metadata.tables.values()
            if other is not target
            for fk in other.foreign_key_constraints
        )

    def _staging_rownum_sequence_ref(self, tablename: str) -> str:
        """Fully schema-qualified, quoted name of tablename's staging _rownum sequence.
        
        Notes
        -----
        DuckDB can't ALTER TABLE ADD COLUMN ... GENERATED ALWAYS AS IDENTITY after creation.
        Pagniation uses a sequence-backed DEFAULT instead to achieve the Postgres-like
        Identity column: a stable, monotonic pagination key populated automatically on insert, independent of
        DuckDB's own rowid (not guaranteed stable across a checkpoint).
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

    @property
    def _commits_between_pagination_batches(self) -> bool:
        """True: bounds how much uncommitted work one load accumulates on a
        large table. Safe here because merge_replace updates matching rows
        in place rather than deleting and reinserting them, so a commit
        between batches never leaves a row removed without its replacement.
        """
        return True

    def check_replace_feasible(
        self,
        table_cls: type["CSVTableProtocol"],
        pk_cols: list[str],
    ) -> None:
        """Raise if merge_strategy='replace' can't update table_cls's own FK columns.

        Notes
        -----
        Meant to be called before any replace load touches table_cls, so a table later 
        in load order failing this check doesn't leave earlier tables already replaced.

        Raises
        ------
        ValueError
            If target is itself referenced by another table's FK and also
            owns an enforced FK of its own on a column being updated: DuckDB
            rejects a SET on any FK-constrained column of a currently-
            referenced row, dropped indexes or not.
        """
        target = table_cls.__table__
        update_cols = [c for c in self._insertable_column_names(table_cls) if c not in pk_cols]
        deferred_cols = {col.name for fk in self.deferred_fk_constraints(table_cls) for col in fk.columns}
        enforced_fk_cols = [
            c for c in update_cols
            if c in target.c and target.c[c].foreign_keys and c not in deferred_cols
        ]
        if enforced_fk_cols and self._is_referenced_by_other_table(target):
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
    ) -> None:
        """Update matching target rows from staging in place, then drop them from staging.

        DuckDB rejects deleting a still-referenced row outright, with no
        FK-toggle or deferred-constraint escape hatch, so replace is an
        UPDATE rather than orm-loader's usual delete-then-reinsert. Matched
        rows are removed from staging so the follow-up merge_insert() call
        only inserts rows that weren't already handled here.

        Only correct when index_strategy drops indexes before this UPDATE:
        updating an indexed column with the index still live raises
        DuckDB's own (opaque) constraint error. capabilities.
        supports_replace_with_kept_indexes is False for this reason; the
        caller is expected to check it before resolving index_strategy to
        "keep" for a "replace" merge.

        Raises
        ------
        ValueError
            See check_replace_feasible().
        """
        self._validate_merge_batch_size(merge_batch_size)
        self.check_replace_feasible(table_cls, pk_cols)
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

        for batch_range in self._paginate_batches(table_cls, staging, session, merge_batch_size):
            _update(batch_range)
            _drop_matched_from_staging(batch_range)
            if batch_range is not None and self._commits_between_pagination_batches:
                session.commit()

    def merge_upsert(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
        pk_cols: list[str],
        *,
        merge_batch_size: int | None = None,
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

        for batch_range in self._paginate_batches(table_cls, staging, session, merge_batch_size):
            select_ = non_paginated_select if batch_range is None else non_paginated_select.where(batch_range)
            session.execute(_upsert(select_))
            if batch_range is not None and self._commits_between_pagination_batches:
                session.commit()

    def merge_insert(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
        *,
        merge_batch_size: int | None = None,
    ) -> None:
        self._validate_merge_batch_size(merge_batch_size)
        target = table_cls.__table__
        staging = table_cls.get_staging_table(session, staging_schema_tag=self.staging_schema_tag)
        insertable_cols = self._insertable_column_names(table_cls)
        non_paginated_select = sa.select(*(staging.c[c] for c in insertable_cols))

        def _insert(select_: Any) -> sa.Insert:
            return sa.insert(target).from_select(insertable_cols, select_)

        for batch_range in self._paginate_batches(table_cls, staging, session, merge_batch_size):
            select_ = non_paginated_select if batch_range is None else non_paginated_select.where(batch_range)
            session.execute(_insert(select_))
            if batch_range is not None and self._commits_between_pagination_batches:
                session.commit()
