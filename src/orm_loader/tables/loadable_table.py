# pyright: reportPrivateUsage=false
import sqlalchemy as sa
import sqlalchemy.orm as so
import logging
from sqlalchemy.exc import InvalidRequestError, UnboundExecutionError

from typing import Type, Any, Iterator
from pathlib import Path
from contextlib import contextmanager
from time import perf_counter

from .orm_table import ORMTableBase
from .typing import CSVTableProtocol
from ..backends.resolve import resolve_backend
from ..loaders.loader_interface import LoaderInterface, LoaderContext, PandasLoader, ParquetLoader

logger = logging.getLogger(__name__)


def _format_elapsed(seconds: float) -> str:
    """Return a compact, human-readable duration for phase logging."""
    return f"{seconds:.2f}s"


def _require_bind(session: so.Session) -> sa.Engine | sa.Connection:
    """Return a bound connectable or raise a stable runtime error."""
    try:
        return session.get_bind()
    except (InvalidRequestError, UnboundExecutionError) as exc:
        raise RuntimeError("Session is not bound to an engine") from exc


"""
CSV Loadable Table Mixins
==================================

This module provides mixins that add staged, file-based ingestion
capabilities to SQLAlchemy ORM-mapped tables.

The functionality here is intentionally infrastructure-focused and
model-agnostic, supporting:
- CSV-based bulk ingestion via staging tables
- optional fast-path database COPY operations
- database-portable merge strategies
- pluggable loader implementations

No schema semantics or domain rules are imposed.
"""


class CSVLoadableTableInterface(ORMTableBase):
    """
    Mixin for ORM tables that support staged CSV-based ingestion.

    This interface implements a database-portable ingestion workflow
    based on temporary staging tables. It supports:
    - dialect-aware staging table creation
    - fast-path COPY-based loading where available
    - ORM-based fallback loading
    - configurable merge strategies
    - explicit staging table lifecycle management

    The class is designed for controlled ingestion pipelines and does
    not attempt to provide concurrency guarantees.
    """

    __abstract__ = True

    @classmethod
    def staging_tablename(cls: Type[CSVTableProtocol]) -> str:
        """
        Return the name of the staging table for this model.

        Returns
        -------
        str
            The staging table name, derived from ``__tablename__``.
        """
        return f"_staging_{cls.__tablename__}"
    
    @classmethod
    def create_staging_table(
        cls: Type[CSVTableProtocol],
        session: so.Session,
    ):
        """
        Create a fresh staging table for ingestion.

        Any existing staging table with the same name is dropped first.
        The staging table schema mirrors the target table schema.

        Parameters
        ----------
        session
            An active SQLAlchemy session bound to an engine.

        Raises
        ------
        RuntimeError
            If the session is not bound to an engine.
        NotImplementedError
            If the database dialect is unsupported.
        """
        _require_bind(session)
        backend = resolve_backend(session)
        backend.create_staging_table(cls, session)

    @classmethod
    @contextmanager
    def manage_indices(
        cls: Type['CSVTableProtocol'],
        session: so.Session,
        index_strategy: str = "auto",
    ) -> Iterator[None]:
        """
        Manage non-primary-key indexes around a staged merge.

        ``index_strategy`` may be ``"auto"``, ``"drop_rebuild"``, or
        ``"keep"``. The backend decides what ``"auto"`` means. At the
        moment SQLite keeps indexes by default, while PostgreSQL drops
        and rebuilds them.
        """
        backend = resolve_backend(session)
        resolved_index_strategy = backend.resolve_index_strategy(index_strategy)
        table_name = cls.__tablename__

        indices = list(cls.__table__.indexes) if resolved_index_strategy == "drop_rebuild" else []
        inspector = sa.inspect(_require_bind(session))
        assert inspector is not None, "Failed to create inspector for index management"
    
        if indices:
            existing_in_db = {idx['name'] for idx in inspector.get_indexes(cls.__tablename__)}
            to_drop = [i for i in indices if i.name in existing_in_db]
            
            if to_drop:
                logger.info(f"Table `{table_name}`: Dropping {len(to_drop)} active indices.")
                drop_started = perf_counter()
                for idx in to_drop:
                    session.execute(sa.schema.DropIndex(idx))
                logger.info(
                    f"Table `{table_name}`: Finished dropping {len(to_drop)} active indices "
                    f"in {_format_elapsed(perf_counter() - drop_started)}."
                )
                logger.info(f"Table `{table_name}`: Committing after index drop.")
                commit_started = perf_counter()
                session.commit()
                logger.info(
                    f"Table `{table_name}`: Commit after index drop completed in "
                    f"{_format_elapsed(perf_counter() - commit_started)}."
                )

        fk_restore_started: float | None = None
        try:
            logger.info(f"Table `{table_name}`: Disabling foreign key checks before merge.")
            fk_disable_started = perf_counter()
            with backend.merge_context(cls, session):
                logger.info(
                    f"Table `{table_name}`: Foreign key checks disabled in "
                    f"{_format_elapsed(perf_counter() - fk_disable_started)}."
                )
                try:
                    yield
                    logger.info(f"Table `{table_name}`: Committing merged rows.")
                    commit_started = perf_counter()
                    session.commit()
                    logger.info(
                        f"Table `{table_name}`: Merge commit completed in "
                        f"{_format_elapsed(perf_counter() - commit_started)}."
                    )
                finally:
                    logger.info(f"Table `{table_name}`: Restoring foreign key checks.")
                    fk_restore_started = perf_counter()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Table `{table_name}`: Merge operation failed - {e}")
            raise
        finally:
            if fk_restore_started is not None:
                logger.info(
                    f"Table `{table_name}`: Foreign key checks restored in "
                    f"{_format_elapsed(perf_counter() - fk_restore_started)}."
                )
            if indices:
                logger.info(f"Table `{table_name}`: Verifying/Rebuilding indices.")
                rebuild_started = perf_counter()
                inspector.clear_cache() # Required to ensure we get the current state of the database after potential changes
                existing_idx_names = {idx['name'] for idx in inspector.get_indexes(table_name)}
               
                for idx in indices:
                    if idx.name not in existing_idx_names:
                        try:
                            logger.info(f"Table `{table_name}`: Restoring missing index: {idx.name}")
                            create_started = perf_counter()
                            session.execute(sa.schema.CreateIndex(idx))
                            logger.info(
                                f"Table `{table_name}`: Restored missing index `{idx.name}` in "
                                f"{_format_elapsed(perf_counter() - create_started)}."
                            )
                            logger.info(f"Table `{table_name}`: Committing restored index `{idx.name}`.")
                            commit_started = perf_counter()
                            session.commit()
                            logger.info(
                                f"Table `{table_name}`: Commit after restoring index `{idx.name}` "
                                f"completed in {_format_elapsed(perf_counter() - commit_started)}."
                           )
                        except Exception as e:
                            session.rollback()
                            logger.error(f"Table `{table_name}`: Failed to restore {idx.name}: {e}")
                    else:
                        logger.debug(f"Table `{table_name}`: Index {idx.name} already exists on disk. Skipping.")
                logger.info(
                    f"Table `{table_name}`: Index verification/rebuild completed in "
                    f"{_format_elapsed(perf_counter() - rebuild_started)}."
                )


    @classmethod
    def get_staging_table(
        cls: Type[CSVTableProtocol],
        session: so.Session,
    ) -> sa.Table:
        """
        Return the reflected staging table, creating it if necessary.

        Parameters
        ----------
        session
            An active SQLAlchemy session bound to an engine.

        Returns
        -------
        sqlalchemy.Table
            The reflected staging table.
        """
        engine = _require_bind(session)
        backend = resolve_backend(session)
        inspector = sa.inspect(engine)
        staging_name = backend.staging_name_for_table(cls.__tablename__)

        if not inspector.has_table(staging_name, schema=backend.staging_schema):
            logger.debug(f"Staging table {staging_name} does not exist; recreating",)
            cls.create_staging_table(session)

        return sa.Table(
            staging_name,
            sa.MetaData(),  # throwaway — keeps staging table out of Base.metadata
            autoload_with=engine,
            schema=backend.staging_schema,
        )

    @classmethod
    def load_staging(
        cls: Type[CSVTableProtocol],
        loader: LoaderInterface,
        loader_context: LoaderContext,
    ) -> int:
        """
        Load data into the staging table.

        This method attempts a fast-path database-native load where
        supported, falling back to an ORM-based loader if necessary.

        Parameters
        ----------
        loader
            Loader implementation used for ORM-based loading.
        loader_context
            Context object containing session, path, and load options.

        Returns
        -------
        int
            Number of rows loaded into the staging table.
        """
        _require_bind(loader_context.session)

        backend = resolve_backend(loader_context.session)
        total = 0

        cls.create_staging_table(loader_context.session)

        try:
            total = backend.load_staging_fast(loader_context=loader_context)
            if total is not None:
                return total
        except Exception as e:
            loader_context.session.rollback()
            logger.warning(f"Fast-path load failed for {cls.__tablename__}: {e}")
            logger.info('Falling back to ORM-based load functionality')

        total = cls.orm_staging_load(
            loader=loader,
            loader_context=loader_context
        )
        return total

    @classmethod
    def orm_staging_load(
        cls: Type[CSVTableProtocol],
        loader: LoaderInterface,
        loader_context: LoaderContext
    ) -> int:
        """
        Load data into the staging table using an ORM-based loader.

        Returns
        -------
        int
            Number of rows loaded.
        """
        return loader.orm_file_load(ctx=loader_context)

    @classmethod
    def _select_loader(cls: Type[CSVTableProtocol], path: Path) -> LoaderInterface:
        """
        Select an appropriate loader based on file type.

        Parameters
        ----------
        path
            Path to the input file.

        Returns
        -------
        LoaderInterface
            A loader instance suitable for the file type.
        """
        suffix = path.suffix.lower()
        if suffix == ".parquet":
            return ParquetLoader()
        else:
            return PandasLoader()


    @classmethod
    def load_csv(
        cls: Type[CSVTableProtocol],
        session: so.Session,
        path: Path,
        *,
        loader: LoaderInterface | None = None,
        normalise: bool = True,
        dedupe: bool = False,
        chunksize: int | None = None,
        merge_strategy: str = "replace",
        quote_mode: str = "auto",
        index_strategy: str = "auto",
        merge_batch_size: int | None = None,
    ) -> int:
        
        """
        Load a CSV (or CSV-like) file into the target table.

        This method orchestrates the full staged ingestion lifecycle:
        - staging table creation
        - file loading
        - merge into the target table
        - staging table cleanup

        Parameters
        ----------
        session
            An active SQLAlchemy session.
        path
            Path to the input CSV or Parquet file.
        loader
            Optional explicit loader instance.
        normalise
            Whether to apply table-level normalisation.
        dedupe
            Whether to deduplicate incoming rows.
        chunksize
            Optional chunk size for incremental loading.
        merge_strategy
            Merge strategy to apply (e.g. ``replace``, ``upsert``, or
            ``insert_if_empty``).
        quote_mode
            Quoting mode used by the PostgreSQL fast-path loader.
        index_strategy
            Index handling strategy during merge. Use ``"auto"`` to let
            the backend choose a sensible default.

        Returns
        -------
        int
            Number of rows loaded into staging before merge.
        """

        logger.debug(f"Table `{cls.__tablename__}`: Loading CSV from {path}")

        if path.stem.lower() != cls.__tablename__:
            raise ValueError(
                f"CSV filename '{path.name}' does not match table '{cls.__tablename__}'"
            )

        if merge_strategy == "insert_if_empty":
            logger.info(
                f"Table `{cls.__tablename__}`: Checking whether target table is empty before staging load."
            )
            check_started = perf_counter()
            has_rows = cls._target_has_rows(
                session=session,
                target=cls.__tablename__,
            )
            logger.info(
                f"Table `{cls.__tablename__}`: Pre-load empty-table check completed in "
                f"{_format_elapsed(perf_counter() - check_started)}."
            )

            if has_rows:
                raise ValueError(
                    f"Table `{cls.__tablename__}` is not empty; cannot use merge strategy "
                    f"'insert_if_empty'"
                )

        loader_context = LoaderContext(
            tableclass=cls,
            session=session,
            path=path,
            staging_table=cls.get_staging_table(session),
            chunksize=chunksize,
            normalise=normalise,
            dedupe=dedupe,
            quote_mode=quote_mode,
        )

        if loader is None:
            loader = cls._select_loader(path)

        # Load to staging (Indices are already excluded via updated create_staging_table)
        logger.info(f"Table `{cls.__tablename__}`: Loading data into staging table")
        total = cls.load_staging(loader=loader, loader_context=loader_context)

        # Merge staging to target (Wrapped in our index dropper!)
        logger.info(f"Table `{cls.__tablename__}`: Merging staging data into target table")
        with cls.manage_indices(session, index_strategy=index_strategy):
            cls.merge_from_staging(session, merge_strategy=merge_strategy, merge_batch_size=merge_batch_size)

        cls.drop_staging_table(session)

        logger.info(f"Table `{cls.__tablename__}`: Successfully finished ingestion. Total rows: {total}")
        return total
        

    @classmethod
    def _target_has_rows(
        cls: Type[CSVTableProtocol],
        session: so.Session,
        target: str,
    ) -> bool:
        """
        Return whether the target table currently contains any rows.
        """
        table = cls.__table__
        if target not in {table.name, table.fullname}:
            table = sa.Table(
                target,
                sa.MetaData(),
                autoload_with=session.get_bind(),
            )
        row = session.execute(
             sa.select(sa.literal(1)).select_from(table).limit(1)
        ).first()
        return row is not None


    @classmethod
    def merge_from_staging(
        cls: Type[CSVTableProtocol],
        session: so.Session,
        merge_strategy: str = "replace",
        *,
        merge_batch_size: int | None = None,
    ):
        """
        Merge data from the staging table into the target table.

        Parameters
        ----------
        session
            An active SQLAlchemy session.
        merge_strategy
            Merge strategy to apply (for example ``replace``,
            ``upsert``, or ``insert_if_empty``).
        """
        target = cls.__tablename__
        pk_cols = cls.pk_names()

        _require_bind(session)
        backend = resolve_backend(session)
        target_empty_confirmed = False
        if merge_strategy in {"replace", "upsert"}:
            logger.info(
                f"Table `{target}`: Checking whether target table is empty for merge optimisation."
            )
            check_started = perf_counter()
            has_rows = cls._target_has_rows(
                session=session,
                target=target,
            )
            logger.info(
                f"Table `{target}`: Empty-table optimisation check completed in "
                f"{_format_elapsed(perf_counter() - check_started)}."
            )
            if not has_rows:
                logger.info(
                    f"Table `{target}`: Target table is empty; routing merge strategy "
                    f"`{merge_strategy}` to insert-if-empty fast path."
                )
                target_empty_confirmed = True
                merge_strategy = "insert_if_empty"

        if merge_strategy == "replace":
            logger.info(f"Table `{target}`: Merge replace delete phase starting.")
            delete_started = perf_counter()
            backend.merge_replace(cls, session, target, pk_cols, merge_batch_size=merge_batch_size)
            logger.info(
                f"Table `{target}`: Merge replace delete phase completed in "
                f"{_format_elapsed(perf_counter() - delete_started)}."
            )
            logger.info(f"Table `{target}`: Merge insert phase starting.")
            insert_started = perf_counter()
            backend.merge_insert(cls, session, target, merge_batch_size=merge_batch_size)
            logger.info(
                f"Table `{target}`: Merge insert phase completed in "
                f"{_format_elapsed(perf_counter() - insert_started)}."
            )
        elif merge_strategy == "upsert":
            logger.info(f"Table `{target}`: Merge upsert phase starting.")
            upsert_started = perf_counter()
            backend.merge_upsert(cls, session, target, pk_cols, merge_batch_size=merge_batch_size)
            logger.info(
                f"Table `{target}`: Merge upsert phase completed in "
                f"{_format_elapsed(perf_counter() - upsert_started)}."
            )
        elif merge_strategy == "insert_if_empty":
            if not target_empty_confirmed:
                logger.info(f"Table `{target}`: Checking whether target table is empty.")
                check_started = perf_counter()
                has_rows = cls._target_has_rows(
                    session=session,
                    target=target,
                )
                logger.info(
                    f"Table `{target}`: Empty-table check completed in "
                    f"{_format_elapsed(perf_counter() - check_started)}."
                )

                if has_rows:
                    raise ValueError(
                        f"Table `{target}` is not empty; cannot use merge strategy "
                        f"'insert_if_empty'"
                    )

            logger.info(f"Table `{target}`: Merge insert-if-empty phase starting.")
            insert_started = perf_counter()
            backend.merge_insert(cls, session, target, merge_batch_size=merge_batch_size)
            logger.info(
                f"Table `{target}`: Merge insert-if-empty phase completed in "
                f"{_format_elapsed(perf_counter() - insert_started)}."
            )
        else:
            raise ValueError(f"Unknown merge strategy '{merge_strategy}'")
    
    @classmethod
    def drop_staging_table(
        cls: Type[CSVTableProtocol],
        session: so.Session,
    ):
        """
        Drop the staging table if it exists.

        Parameters
        ----------
        session
            An active SQLAlchemy session bound to an engine.
        """
        backend = resolve_backend(session)
        backend.drop_staging_table(cls, session)

    @classmethod
    def csv_columns(cls) -> dict[str, sa.ColumnElement[Any]]:
        """
        Return a mapping of CSV column names to model columns.

        By default this is equivalent to :meth:`model_columns`.
        Override this method to implement custom column mappings.

        Returns
        -------
        dict[str, sqlalchemy.ColumnElement]
            Mapping of input column names to SQLAlchemy columns.
        """
        cols = cls.model_columns()
        computed_names = {c.name for c in cls.__table__.columns if c.computed is not None}  # type: ignore
        return {k: v for k, v in cols.items() if k not in computed_names}
