from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Type

import duckdb_sqlalchemy
import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import qualified
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
        *own* table's bucket unless the column declares ``use_alter=True`` --
        without that, this method can't see it, and it will insert without
        error instead of being caught by validate_deferred_fks(). Model
        authors: mark a self-referential FK ``use_alter=True`` explicitly if
        it needs anti-join validation on DuckDB.
        """
        metadata = table_cls.__table__.metadata
        entries = sa.schema.sort_tables_and_constraints(metadata.tables.values())
        deferred = next(fks for tbl, fks in entries if tbl is None)
        return tuple(fk for fk in deferred if fk.table is table_cls.__table__)

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
        staging_ref = self.qualified_staging_name(table_cls.__tablename__)
        seq_ref = self._staging_rownum_sequence_ref(table_cls.__tablename__)
        session.execute(sa.text(f'DROP TABLE IF EXISTS {staging_ref};'), bind_arguments=bind_arguments)
        session.execute(sa.text(f'DROP SEQUENCE IF EXISTS {seq_ref};'), bind_arguments=bind_arguments)

        staging_name = self.staging_name_for_table(table_cls.__tablename__)
        metadata = sa.MetaData()
        staging_columns = [
            sa.Column(col.name, col.type, nullable=True)
            for col in table_cls.__table__.columns
        ]
        staging_table = sa.Table(staging_name, metadata, *staging_columns, schema=self.staging_schema)
        metadata.create_all(
            bind=session.connection(bind_arguments=bind_arguments), tables=[staging_table]
        )

        session.execute(sa.text(f'CREATE SEQUENCE {seq_ref};'), bind_arguments=bind_arguments)
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
        session.execute(
            sa.text(f'DROP TABLE IF EXISTS {self.qualified_staging_name(table_cls.__tablename__)}'),
            bind_arguments=bind_arguments,
        )
        session.execute(
            sa.text(f'DROP SEQUENCE IF EXISTS {self._staging_rownum_sequence_ref(table_cls.__tablename__)};'),
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

    def _staging_rownum_index(
        self, table_cls: type["CSVTableProtocol"], staging: sa.Table, session: so.Session
    ) -> None:
        """Index staging's _rownum column for O(N log N) windowed range scans."""
        staging_name = self.staging_name_for_table(table_cls.__tablename__)
        idx = sa.Index(f"{staging_name}_rownum_idx", staging.c._rownum)
        idx.create(bind=session.connection(bind_arguments={"mapper": table_cls}), checkfirst=True)
        session.commit()

    @staticmethod
    def _staging_rownum_bound(staging: sa.Table, session: so.Session) -> int:
        """Highest _rownum in staging, as the pagination upper bound (0 when empty)."""
        return session.execute(
            sa.select(sa.func.coalesce(sa.func.max(staging.c._rownum), 0)).select_from(staging)
        ).scalar_one()

    def _staging_rownum_batches(
        self,
        table_cls: type["CSVTableProtocol"],
        staging: sa.Table,
        session: so.Session,
        merge_batch_size: int,
    ) -> Iterator[sa.ColumnElement[bool]]:
        """Yield successive _rownum window predicates covering every row in
        staging, each at most merge_batch_size wide.
        """
        self._staging_rownum_index(table_cls, staging, session)
        bound = self._staging_rownum_bound(staging, session)
        start = 0
        while start < bound:
            end = start + merge_batch_size
            yield sa.and_(staging.c._rownum > start, staging.c._rownum <= end)
            start = end

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
        """
        self._validate_merge_batch_size(merge_batch_size)
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

        if merge_batch_size is None:
            _update()
            _drop_matched_from_staging()
            return

        for batch_range in self._staging_rownum_batches(table_cls, staging, session, merge_batch_size):
            _update(batch_range)
            _drop_matched_from_staging(batch_range)
            session.commit()

    def merge_upsert(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
        pk_cols: list[str],
        *,
        merge_batch_size: int | None = None,
    ) -> None:
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

        if merge_batch_size is None:
            session.execute(_upsert(non_paginated_select))
            return

        for batch_range in self._staging_rownum_batches(table_cls, staging, session, merge_batch_size):
            session.execute(_upsert(non_paginated_select.where(batch_range)))

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

        if merge_batch_size is None:
            session.execute(_insert(non_paginated_select))
            return

        for batch_range in self._staging_rownum_batches(table_cls, staging, session, merge_batch_size):
            session.execute(_insert(non_paginated_select.where(batch_range)))
