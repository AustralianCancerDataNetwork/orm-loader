# pyright: reportPrivateUsage=false
import sqlalchemy as sa
import sqlalchemy.orm as so
import logging
from sqlalchemy.exc import InvalidRequestError, UnboundExecutionError

from typing import Type, ClassVar, Optional, Any, Iterator
from pathlib import Path
from contextlib import contextmanager

from .orm_table import ORMTableBase
from .typing import CSVTableProtocol
from ..backends.resolve import resolve_backend
from ..loaders.loader_interface import LoaderInterface, LoaderContext, PandasLoader, ParquetLoader

logger = logging.getLogger(__name__)


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
    _staging_tablename: ClassVar[Optional[str]] = None

    @classmethod
    def staging_tablename(cls: Type[CSVTableProtocol]) -> str:
        """
        Return the name of the staging table for this model.

        If a custom staging table name has been set on the class, it is
        used; otherwise a default name derived from ``__tablename__``
        is returned.

        Returns
        -------
        str
            The staging table name.
        """
        if cls._staging_tablename:  # type: ignore
            return cls._staging_tablename
        return f"_staging_{cls.__tablename__}"
    
    @classmethod
    def create_staging_table(
        cls: Type[CSVTableProtocol], 
        session: so.Session
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
        backend.create_staging_table(cls, session, cls.staging_tablename())

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

        indices = list(cls.__table__.indexes) if resolved_index_strategy == "drop_rebuild" else []
        inspector = sa.inspect(_require_bind(session))
        assert inspector is not None, "Failed to create inspector for index management"
    
        if indices:
            existing_in_db = {idx['name'] for idx in inspector.get_indexes(cls.__tablename__)}
            to_drop = [i for i in indices if i.name in existing_in_db]
            
            if to_drop:
                logger.info(f"Dropping {len(to_drop)} active indices...")
                for idx in to_drop:
                    session.execute(sa.schema.DropIndex(idx))
                session.commit()

        try:
            with backend.merge_context(cls, session):
                yield 
                session.commit() 
            
        except Exception as e:
            session.rollback()
            logger.error(f"Table `{cls.__tablename__}`: Merge operation failed - {e}")
            raise
        finally:
           if indices:
               logger.info(f"Table `{cls.__tablename__}`: Verifying/Rebuilding indices.")
               inspector.clear_cache() # Required to ensure we get the current state of the database after potential changes
               existing_idx_names = {idx['name'] for idx in inspector.get_indexes(cls.__tablename__)}
               
               for idx in indices:
                   if idx.name not in existing_idx_names:
                       try:
                           logger.info(f"Restoring missing index: {idx.name}")
                           session.execute(sa.schema.CreateIndex(idx))
                           session.commit()
                       except Exception as e:
                           session.rollback()
                           logger.error(f"Failed to restore {idx.name}: {e}")
                   else:
                       logger.debug(f"Index {idx.name} actually exists on disk. Skipping.")


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
        inspector = sa.inspect(engine)
        staging_name = cls.staging_tablename()

        if not inspector.has_table(staging_name):
            logger.debug(f"Staging table {staging_name} does not exist; recreating",)
            cls.create_staging_table(session)

        return sa.Table(
            staging_name,
            cls.metadata,
            autoload_with=engine,
        )

    @classmethod   
    def load_staging(
        cls: Type[CSVTableProtocol],
        loader: LoaderInterface,
        loader_context: LoaderContext
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

        try:
            cls.create_staging_table(loader_context.session)

            try:
                total = backend.load_staging_fast(
                    loader_context=loader_context,
                    staging_name=cls.staging_tablename(),
                )
                if total is not None:
                    return total
            except Exception as e:
                loader_context.session.rollback()
                logger.warning(f"Fast-path load failed for {cls.staging_tablename()}: {e}")
                logger.info('Falling back to ORM-based load functionality')

            total = cls.orm_staging_load(
                loader=loader,
                loader_context=loader_context
            )   
        finally:
            cls._staging_tablename = None
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
        quote_mode: str = "csv",
        index_strategy: str = "auto",
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
            Merge strategy to apply (e.g. ``replace`` or ``upsert``).
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
            cls.merge_from_staging(session, merge_strategy=merge_strategy)
        
        cls.drop_staging_table(session)

        logger.info(f"Table `{cls.__tablename__}`: Successfully finished ingestion. Total rows: {total}")
        return total
        

    @classmethod
    def _merge_replace(
        cls: Type[CSVTableProtocol],
        session: so.Session, 
        target: str, 
        staging: str, 
        pk_cols: list[str]
    ):
        """
        Merge staging data by replacing existing rows.

        Existing target rows matching the staging primary keys are
        deleted prior to insertion.
        """
        backend = resolve_backend(session)
        backend.merge_replace(cls, session, target, staging, pk_cols)

    @classmethod
    def _merge_insert(
        cls: Type[CSVTableProtocol],
        session: so.Session,
        target: str,
        staging: str
    ):
        """
        Insert all rows from the staging table into the target table.
        """
        backend = resolve_backend(session)
        backend.merge_insert(cls, session, target, staging)


    @classmethod
    def _merge_upsert(
        cls: Type[CSVTableProtocol], 
        session: so.Session, 
        target: str, 
        staging: str, 
        pk_cols: list[str]
    ):
        """
        Merge staging data using an upsert strategy.
        """
        backend = resolve_backend(session)
        backend.merge_upsert(cls, session, target, staging, pk_cols)

    @classmethod
    def merge_from_staging(
        cls: Type[CSVTableProtocol], 
        session: so.Session, 
        merge_strategy: str = "replace"
    ):
        """
        Merge data from the staging table into the target table.

        Parameters
        ----------
        session
            An active SQLAlchemy session.
        merge_strategy
            Merge strategy to apply.
        """
        target = cls.__tablename__
        staging = cls.staging_tablename()
        pk_cols = cls.pk_names()

        _require_bind(session)
        if merge_strategy == "replace":
            cls._merge_replace(
                session=session,
                target=target,
                staging=staging,
                pk_cols=pk_cols,
            )
            cls._merge_insert(
                session=session,
                target=target,
                staging=staging,
            )
        elif merge_strategy == "upsert":
            cls._merge_upsert(
                session=session,
                target=target,
                staging=staging,
                pk_cols=pk_cols,
            )
        else:
            raise ValueError(f"Unknown merge strategy '{merge_strategy}'")
    
    @classmethod
    def drop_staging_table(cls: Type[CSVTableProtocol], session: so.Session):
        """
        Drop the staging table if it exists.
        """
        backend = resolve_backend(session)
        backend.drop_staging_table(session, cls.staging_tablename())

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
