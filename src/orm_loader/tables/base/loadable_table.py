import sqlalchemy as sa
import sqlalchemy.orm as so
import pandas as pd
import pyarrow as pa
import logging

from typing import Type, ClassVar, Optional, Any
from pathlib import Path

from .orm_table import ORMTableBase
from .typing import CSVTableProtocol
from ...loaders.loader_interface import LoaderInterface, LoaderContext, PandasLoader, ParquetLoader
from ...loaders.loading_helpers import quick_load_pg

logger = logging.getLogger(__name__)

class CSVLoadableTableInterface(ORMTableBase):
    """
    Mixin for ORM tables that can be loaded from CSV files.
    """

    __abstract__ = True
    _staging_tablename: ClassVar[Optional[str]] = None

    @classmethod
    def staging_tablename(cls: Type[CSVTableProtocol]) -> str:
        if cls._staging_tablename:
            return cls._staging_tablename
        return f"_staging_{cls.__tablename__}"
    
    @classmethod
    def create_staging_table(
        cls: Type[CSVTableProtocol], 
        session: so.Session
    ):
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
            session.execute(sa.text(f'''
                CREATE TABLE "{cls.staging_tablename()}" AS
                SELECT * FROM "{table.name}" WHERE 0;
            '''))
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
        Return the reflected staging table, creating it if it does not exist.
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
        return loader.orm_file_load(ctx=loader_context)

    @classmethod
    def _select_loader(cls: Type[CSVTableProtocol], path: Path) -> LoaderInterface:
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
        session.execute(
            sa.text(f'DROP TABLE IF EXISTS "{cls.staging_tablename()}"')
        )

    @classmethod
    def csv_columns(cls) -> dict[str, sa.ColumnElement]:
        """
        Return a mapping of CSV column names to SQLAlchemy columns.
        By default, this is the same as model_columns().
        Override this method to provide custom mappings.
        """
        return cls.model_columns()
    
class ParquetLoadableTableMixin(ORMTableBase):
    """
    Mixin for ORM tables that can be loaded from Parquet files.
    """

    __abstract__ = True

    @classmethod
    def load_parquet(
        cls,
        session: so.Session,
        path: Path,
        *,
        columns: list[str] | None = None,
        filters: list[tuple] | None = None,
        commit_on_chunk: bool = False,
    ) -> int:
        raise NotImplementedError("Parquet loading not implemented for this table")

