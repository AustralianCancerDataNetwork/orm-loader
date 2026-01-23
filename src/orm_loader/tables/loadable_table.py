import sqlalchemy as sa
import sqlalchemy.orm as so
import logging

from typing import Type, ClassVar, Optional
from pathlib import Path

from .orm_table import ORMTableBase
from .typing import CSVTableProtocol
from ..loaders.loader_interface import LoaderInterface, LoaderContext, PandasLoader, ParquetLoader
from ..loaders.loading_helpers import quick_load_pg

logger = logging.getLogger(__name__)


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
        if cls._staging_tablename:
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
        table = cls.__table__
        session.execute(sa.text(f"""
            DROP TABLE IF EXISTS "{cls.staging_tablename()}";
        """))

        if session.bind is None:
            raise RuntimeError("Session is not bound to an engine")
        
        dialect = session.bind.dialect.name 

        if dialect == "postgresql":
            session.execute(sa.text(f'''
                CREATE UNLOGGED TABLE "{cls.staging_tablename()}"
                (LIKE "{table.name}" INCLUDING ALL);
            '''))
        elif dialect == "sqlite":

            metadata = sa.MetaData()

            staging_columns = []
            for col in table.columns:
                staging_columns.append(
                    sa.Column(
                        col.name,
                        col.type,          
                        nullable=True,     
                    )
                )

            staging_table = sa.Table(
                cls.staging_tablename(),
                metadata,
                *staging_columns,
            )

            conn = session.connection()
            metadata.create_all(bind=conn, tables=[staging_table])
            # this borks on date cols because it loses the date 
            # specification and reverts to NUM
            # - changing to metadata.create_all approach for sqlite
            # but not postgresql for now to keep unlogged table feature
            # session.execute(sa.text(f'''
            #     CREATE TABLE "{cls.staging_tablename()}" AS
            #     SELECT * FROM "{table.name}" WHERE 0;
            # '''))
        else:
            raise NotImplementedError(
                f"Staging table creation not implemented for dialect '{dialect}'"
            )
        # query the sense of having internal commit here, but for now 
        # it is required for the ORM-based fallback loader to function 
        # cleanly for external pipeline purposes

        session.commit()


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
        if session.bind is None:
            raise RuntimeError("Session is not bound to an engine")

        engine = session.get_bind()
        inspector = sa.inspect(engine)
        staging_name = cls.staging_tablename()

        if not inspector.has_table(staging_name):
            logger.warning(f"Staging table {staging_name} does not exist; recreating",)
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
        if loader_context.session.bind is None:
            raise RuntimeError("Session is not bound to an engine")

        dialect = loader_context.session.bind.dialect.name
        total = 0

        try:
            cls.create_staging_table(loader_context.session)

            if dialect == "postgresql":
                try:
                    total = quick_load_pg(
                        path=loader_context.path,
                        session=loader_context.session,
                        tablename=cls.staging_tablename(),
                    )
                    return total
                except Exception as e:
                    logger.warning(f"COPY failed for {cls.staging_tablename()}: {e}")
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
        dedupe_incl_db: bool = False,
        merge_strategy: str = "replace",
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
        dedupe_incl_db
            Whether deduplication should include existing database rows.
        merge_strategy
            Merge strategy to apply (e.g. ``replace`` or ``upsert``).

        Returns
        -------
        int
            Number of rows loaded.
        """

        logger.debug(f"Loading CSV for {cls.__tablename__} via staging table from {path}")

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
            dedupe_incl_db=dedupe_incl_db,
        )

        if loader is None:
            loader = cls._select_loader(path)
        total = cls.load_staging(loader=loader, loader_context=loader_context)

        cls.merge_from_staging(session, merge_strategy=merge_strategy)
        cls.drop_staging_table(session)

        return total
        

    @classmethod
    def _merge_replace(
        cls: Type[CSVTableProtocol],
        session: so.Session, 
        target: str, 
        staging: str, 
        pk_cols: list[str],
        dialect: str
    ):
        """
        Merge staging data by replacing existing rows.

        Existing target rows matching the staging primary keys are
        deleted prior to insertion.
        """
        if dialect == "postgresql":
            pk_join = " AND ".join(
                f't."{c}" = s."{c}"' for c in pk_cols
            )

            session.execute(sa.text(f"""
                DELETE FROM "{target}" t
                USING "{staging}" s
                WHERE {pk_join};
            """))

        elif dialect == "sqlite":
            if len(pk_cols) == 1:
                pk = pk_cols[0]
                session.execute(sa.text(f"""
                    DELETE FROM "{target}"
                    WHERE "{pk}" IN (
                        SELECT "{pk}" FROM "{staging}"
                    );
                """))
            else:
                pk_match = " AND ".join(
                    f'"{target}"."{c}" = "{staging}"."{c}"' for c in pk_cols
                )
                session.execute(sa.text(f"""
                    DELETE FROM "{target}"
                    WHERE EXISTS (
                        SELECT 1 FROM "{staging}"
                        WHERE {pk_match}
                    );
                """))

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
        session.execute(sa.text(f"""
            INSERT INTO "{target}"
            SELECT * FROM "{staging}";
        """))


    @classmethod
    def _merge_upsert(
        cls: Type[CSVTableProtocol], 
        session: so.Session, 
        target: str, 
        staging: str, 
        pk_cols: list[str],
        dialect: str
    ):
        """
        Merge staging data using an upsert strategy.
        """
        if dialect == "postgresql":
            # INSERT … ON CONFLICT DO NOTHING
            session.execute(sa.text(f"""
                INSERT INTO "{target}"
                SELECT * FROM "{staging}"
                ON CONFLICT ({", ".join(f'"{c}"' for c in pk_cols)}) DO NOTHING;
            """))

        elif dialect == "sqlite":
            session.execute(sa.text(f"""
                INSERT OR IGNORE INTO "{target}"
                SELECT * FROM "{staging}";
            """))

        else:
            raise NotImplementedError

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

        if not session.bind:
            raise RuntimeError("Session is not bound to an engine")
        
        dialect = session.bind.dialect.name
        if merge_strategy == "replace":
            cls._merge_replace(
                session=session,
                target=target,
                staging=staging,
                pk_cols=pk_cols,
                dialect=dialect,
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
                dialect=dialect,
            )
        else:
            raise ValueError(f"Unknown merge strategy '{merge_strategy}'")
    
    @classmethod
    def drop_staging_table(cls: Type[CSVTableProtocol], session: so.Session):
        """
        Drop the staging table if it exists.
        """
        session.execute(
            sa.text(f'DROP TABLE IF EXISTS "{cls.staging_tablename()}"')
        )

    @classmethod
    def csv_columns(cls) -> dict[str, sa.ColumnElement]:
        """
        Return a mapping of CSV column names to model columns.

        By default this is equivalent to :meth:`model_columns`.
        Override this method to implement custom column mappings.

        Returns
        -------
        dict[str, sqlalchemy.ColumnElement]
            Mapping of input column names to SQLAlchemy columns.
        """
        return cls.model_columns()
    