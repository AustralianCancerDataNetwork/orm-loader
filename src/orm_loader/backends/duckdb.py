from __future__ import annotations

from typing import TYPE_CHECKING, Any, Type

import duckdb_sqlalchemy
import sqlalchemy as sa
import sqlalchemy.orm as so
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
        """
        metadata = table_cls.__table__.metadata
        entries = sa.schema.sort_tables_and_constraints(metadata.tables.values())
        deferred = next(fks for tbl, fks in entries if tbl is None)
        return tuple(fk for fk in deferred if fk.table is table_cls.__table__)

    def create_staging_table(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
    ) -> None:
        bind_arguments = {"mapper": table_cls}
        session.execute(
            sa.text(f'DROP TABLE IF EXISTS {self.qualified_staging_name(table_cls.__tablename__)};'),
            bind_arguments=bind_arguments,
        )

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
        session.commit()

    def drop_staging_table(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
    ) -> None:
        session.execute(
            sa.text(f'DROP TABLE IF EXISTS {self.qualified_staging_name(table_cls.__tablename__)}'),
            bind_arguments={"mapper": table_cls},
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

    @staticmethod
    def _staging_rowid() -> sa.ColumnElement[int]:
        """DuckDB's own rowid: dense per table, but not guaranteed to start at 0 or 1."""
        return sa.literal_column("rowid")

    def _staging_rowid_bounds(self, staging: sa.Table, session: so.Session) -> tuple[int, int] | None:
        """Lowest and highest rowid in *staging*, or None when empty.

        DuckDB's rowid isn't guaranteed to start at any fixed value (verified
        live), unlike SQLite's, so pagination bounds against the real
        min/max instead of assuming a 0-based range.
        """
        lowest, highest = session.execute(
            sa.select(sa.func.min(self._staging_rowid()), sa.func.max(self._staging_rowid()))
            .select_from(staging)
        ).one()
        if lowest is None:
            return None
        return lowest, highest

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

        bounds = self._staging_rowid_bounds(staging, session)
        if bounds is None:
            return
        start, maximum = bounds
        start -= 1
        rowid = self._staging_rowid()
        while start < maximum:
            end = start + merge_batch_size
            batch_range = sa.and_(rowid > start, rowid <= end)
            _update(batch_range)
            _drop_matched_from_staging(batch_range)
            session.commit()
            start = end

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

        bounds = self._staging_rowid_bounds(staging, session)
        if bounds is None:
            return
        start, maximum = bounds
        start -= 1
        rowid = self._staging_rowid()
        while start < maximum:
            end = start + merge_batch_size
            batch_select = non_paginated_select.where(rowid > start, rowid <= end)
            session.execute(_upsert(batch_select))
            start = end

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

        bounds = self._staging_rowid_bounds(staging, session)
        if bounds is None:
            return
        start, maximum = bounds
        start -= 1
        rowid = self._staging_rowid()
        while start < maximum:
            end = start + merge_batch_size
            batch_select = non_paginated_select.where(rowid > start, rowid <= end)
            session.execute(_insert(batch_select))
            start = end
