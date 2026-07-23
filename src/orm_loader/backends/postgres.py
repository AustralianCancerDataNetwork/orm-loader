from __future__ import annotations

from contextlib import AbstractContextManager, contextmanager
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
import sqlalchemy.event as sae
import sqlalchemy.orm as so

from ..loaders.loading_helpers import quick_load_pg
from .base import BackendCapabilities, DatabaseBackend, Dialect, STAGING_SCHEMA

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine

    from ..loaders.data_classes import LoaderContext
    from ..tables.typing import CSVTableProtocol

_VALID_PG_REPLICATION_ROLES = frozenset({"origin", "local", "replica"})


class PostgresBackend(DatabaseBackend):
    def __init__(self, *, staging_schema: str | None = STAGING_SCHEMA) -> None:
        super().__init__(staging_schema=staging_schema)

    @staticmethod
    def staging_name_for_table(tablename: str) -> str:
        return f"_staging_{tablename}"

    @property
    def name(self) -> str:
        return "postgres"

    @property
    def dialect(self) -> Dialect:
        return Dialect.POSTGRESQL

    @property
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            supports_fast_load=True,
            supports_unlogged_staging=True,
            supports_fk_toggle=True,
            supports_materialized_views=True,
        )

    def create_staging_table(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
    ) -> None:
        table = table_cls.__table__
        staging_ref = self.qualified_staging_name(table_cls.__tablename__)
        session.execute(sa.text(f'DROP TABLE IF EXISTS {staging_ref};'))
        session.execute(
            sa.text(
                f'''
                CREATE UNLOGGED TABLE {staging_ref}
                (LIKE "{table.name}" INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
                '''
            )
        )

        computed_cols = [c.name for c in table.columns if c.computed is not None]
        for col in computed_cols:
            session.execute(sa.text(f'ALTER TABLE {staging_ref} DROP COLUMN "{col}";'))

        # allows pagination in O(N log N) time for large tables in merge_insert without needing to add an index on every staging table
        session.execute(
            sa.text(
                f'ALTER TABLE {staging_ref} ADD COLUMN _rownum BIGINT'
                f" GENERATED ALWAYS AS IDENTITY (CACHE 1000);"
            )
        )

        session.commit()

    def drop_staging_table(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
    ) -> None:
        session.execute(sa.text(f'DROP TABLE IF EXISTS {self.qualified_staging_name(table_cls.__tablename__)}'))

    def load_staging_fast(
        self,
        loader_context: "LoaderContext",
    ) -> int | None:
        tablename = loader_context.tableclass.__tablename__
        return quick_load_pg(
            path=loader_context.path,
            session=loader_context.session,
            tablename=self.staging_name_for_table(tablename),
            schema=self.staging_schema,
            quote_mode=loader_context.quote_mode,
        )

    @staticmethod
    def _normalize_fk_check_state(previous_state: str | int) -> str:
        if isinstance(previous_state, int):
            raise ValueError(
                f"Invalid PostgreSQL session_replication_role {previous_state!r}: "
                "Postgres uses string roles ('origin', 'local', 'replica'), not integers. "
                "The value passed here should always come from this backend's own "
                "disable_fk_check(), which returns a string."
            )
        normalised = previous_state.strip().lower()
        if normalised not in _VALID_PG_REPLICATION_ROLES:
            raise ValueError(
                f"Invalid PostgreSQL session_replication_role {previous_state!r}. "
                f"Expected one of: {sorted(_VALID_PG_REPLICATION_ROLES)}"
            )
        return normalised

    def disable_fk_check(self, session: so.Session) -> str | int:
        previous_state = session.execute(sa.text("SHOW session_replication_role")).scalar()
        session.execute(sa.text("SET session_replication_role = 'replica'"))
        if not isinstance(previous_state, str):
            raise RuntimeError("Expected PostgreSQL FK state to be a string")
        return previous_state

    def enable_fk_check(self, session: so.Session) -> str | int:
        previous_state = session.execute(sa.text("SHOW session_replication_role")).scalar()
        session.execute(sa.text("SET session_replication_role = 'origin'"))
        if not isinstance(previous_state, str):
            raise RuntimeError("Expected PostgreSQL FK state to be a string")
        return previous_state

    def restore_fk_check(
        self,
        session: so.Session,
        previous_state: str | int,
    ) -> None:
        safe_state = self._normalize_fk_check_state(previous_state)
        session.execute(sa.text(f"SET session_replication_role = '{safe_state}'"))

    def merge_replace(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
        target_name: str,
        pk_cols: list[str],
        *,
        merge_batch_size: int | None = None,
    ) -> None:
        staging_ref = self.qualified_staging_name(table_cls.__tablename__)
        pk_join = " AND ".join(f't."{c}" = s."{c}"' for c in pk_cols)

        non_paginated_replace = sa.text(
            f'DELETE FROM "{target_name}" t USING {staging_ref} s WHERE {pk_join}'
        )

        if merge_batch_size is None:
            session.execute(non_paginated_replace)
            return

        total = session.execute(sa.text(f'SELECT COUNT(*) FROM {staging_ref}')).scalar_one()
        if total <= merge_batch_size:
            session.execute(non_paginated_replace)
            return

        staging_name = self.staging_name_for_table(table_cls.__tablename__)
        session.execute(sa.text(f'CREATE INDEX IF NOT EXISTS "{staging_name}_rownum_idx" ON {staging_ref} (_rownum)'))
        session.commit()

        start = 0
        while start < total:
            end = start + merge_batch_size
            session.execute(
                sa.text(
                    f'DELETE FROM "{target_name}" t USING {staging_ref} s'
                    f' WHERE {pk_join} AND s._rownum > :start AND s._rownum <= :end'
                ),
                {"start": start, "end": end},
            )
            session.commit()
            start = end

    def merge_upsert(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
        target_name: str,
        pk_cols: list[str],
        *,
        merge_batch_size: int | None = None,
    ) -> None:
        staging_ref = self.qualified_staging_name(table_cls.__tablename__)
        insertable_cols = self._insertable_column_names(table_cls)
        cols_str = ", ".join(f'"{c}"' for c in insertable_cols)
        conflict_cols = ", ".join(f'"{c}"' for c in pk_cols)

        non_paginated_upsert = sa.text(
            f'INSERT INTO "{target_name}" ({cols_str})'
            f' SELECT {cols_str} FROM {staging_ref}'
            f' ON CONFLICT ({conflict_cols}) DO NOTHING'
        )

        if merge_batch_size is None:
            session.execute(non_paginated_upsert)
            return

        total = session.execute(sa.text(f'SELECT COUNT(*) FROM {staging_ref}')).scalar_one()
        if total <= merge_batch_size:
            session.execute(non_paginated_upsert)
            return

        staging_name = self.staging_name_for_table(table_cls.__tablename__)
        session.execute(sa.text(f'CREATE INDEX IF NOT EXISTS "{staging_name}_rownum_idx" ON {staging_ref} (_rownum)'))
        session.commit()

        start = 0
        while start < total:
            end = start + merge_batch_size
            session.execute(
                sa.text(
                    f'INSERT INTO "{target_name}" ({cols_str})'
                    f' SELECT {cols_str} FROM {staging_ref}'
                    f' WHERE _rownum > :start AND _rownum <= :end'
                    f' ON CONFLICT ({conflict_cols}) DO NOTHING'
                ),
                {"start": start, "end": end},
            )
            session.commit()
            start = end

    def merge_insert(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
        target_name: str,
        *,
        merge_batch_size: int | None = None,
    ) -> None:
        staging_ref = self.qualified_staging_name(table_cls.__tablename__)
        insertable_cols = self._insertable_column_names(table_cls)
        cols_str = ", ".join(f'"{c}"' for c in insertable_cols)

        non_paginated_insert = sa.text(
            f'INSERT INTO "{target_name}" ({cols_str})'
            f' SELECT {cols_str} FROM {staging_ref}'
        )

        if merge_batch_size is None:
            session.execute(non_paginated_insert)
            return

        total = session.execute(sa.text(f'SELECT COUNT(*) FROM {staging_ref}')).scalar_one()
        if total <= merge_batch_size:
            session.execute(non_paginated_insert)
            return

        # Paginated path: index _rownum for O(N log N) range scans then
        # INSERT in batch-sized transactions to bound WAL per commit.
        # session_replication_role='replica' is session-level and persists
        # across commits, so FK checks stay disabled for all batches.
        staging_name = self.staging_name_for_table(table_cls.__tablename__)
        session.execute(sa.text(f'CREATE INDEX IF NOT EXISTS "{staging_name}_rownum_idx" ON {staging_ref} (_rownum)'))
        session.commit()

        start = 0
        while start < total:
            end = start + merge_batch_size
            session.execute(
                sa.text(
                    f'INSERT INTO "{target_name}" ({cols_str})'
                    f' SELECT {cols_str} FROM {staging_ref}'
                    f' WHERE _rownum > :start AND _rownum <= :end'
                ),
                {"start": start, "end": end},
            )
            session.commit()
            start = end

    def merge_context(
        self,
        table_cls: type["CSVTableProtocol"],
        session: so.Session,
    ) -> AbstractContextManager[None]:
        return self.bulk_load_context(session, disable_fk=True, no_autoflush=False)

    def create_materialized_view(
        self,
        bind: Engine | Connection,
        name: str,
        selectable: sa.sql.Select[Any],
    ) -> None:
        from ..mappers.materialised_view_mixin import CreateMaterializedView

        with self._as_connection(bind) as conn:
            conn.execute(CreateMaterializedView(name, selectable))

    def refresh_materialized_view(
        self,
        bind: Engine | Connection,
        name: str,
    ) -> None:
        with self._as_connection(bind) as conn:
            safe_name = name
            dialect = getattr(conn, "dialect", None)
            if dialect is not None:
                safe_name = dialect.identifier_preparer.quote(name)
            conn.execute(sa.text(f"REFRESH MATERIALIZED VIEW {safe_name};"))

    @contextmanager
    def engine_with_replica_role(self, engine: "Engine"):
        def _set_replica_role(
            dbapi_conn: sa.engine.interfaces.DBAPIConnection,
            _,
        ) -> None:
            cur = dbapi_conn.cursor()
            cur.execute("SET session_replication_role = 'replica'")
            cur.close()

        sae.listen(engine, "connect", _set_replica_role)

        try:
            yield engine
        finally:
            sae.remove(engine, "connect", _set_replica_role)
            with engine.connect() as conn:
                conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                conn.execute(sa.text("SET session_replication_role = DEFAULT"))
                role = conn.execute(sa.text("SHOW session_replication_role")).scalar()
                if role != "origin":
                    raise RuntimeError("Failed to restore session_replication_role")
