from typing import Protocol, ClassVar, runtime_checkable, TYPE_CHECKING, Optional, Type, Dict, Any
import sqlalchemy.orm as so
import sqlalchemy as sa
import pandas as pd
from pathlib import Path
if TYPE_CHECKING:
    from ...loaders import LoaderContext, LoaderInterface

@runtime_checkable
class ORMTableProtocol(Protocol):
    
    """
    Structural protocol for ORM-mapped *table classes*.
    """

    __tablename__: ClassVar[str]
    __table__: ClassVar[sa.Table]
    metadata: ClassVar[sa.MetaData]

    @classmethod
    def mapper_for(cls) -> so.Mapper: ...

    @classmethod
    def pk_names(cls) -> list[str]: ...

    @classmethod
    def pk_columns(cls) -> list[sa.ColumnElement]: ...

    @classmethod
    def model_columns(cls) -> dict[str, sa.ColumnElement]: ...


@runtime_checkable
class CSVTableProtocol(ORMTableProtocol, Protocol):
    """
    Protocol for ORM tables that support CSV-based ingestion.
    """

    _staging_tablename: ClassVar[Optional[str]] = None

    @classmethod
    def staging_tablename(cls) -> str: ...

    @classmethod
    def _select_loader(cls, path: Path) -> "LoaderInterface": ...

    @classmethod
    def create_staging_table(cls, session: so.Session) -> None: ...

    @classmethod
    def load_staging(cls: Type["CSVTableProtocol"], loader: "LoaderInterface", loader_context: "LoaderContext") -> int: ...

    @classmethod
    def load_csv(
        cls, 
        session: so.Session, 
        path: Path, 
        *, 
        normalise: bool = True, 
        dedupe: bool = False, 
        chunksize: int | None = None, 
        merge_strategy: str = "replace", 
        dedupe_incl_db: bool = False
    ) -> int: ...

    @classmethod
    def orm_staging_load(cls, loader: "LoaderInterface",loader_context: "LoaderContext") -> int: ...

    @classmethod
    def get_staging_table(cls, session: so.Session) -> sa.Table: ...

    @classmethod
    def merge_from_staging(cls, session: so.Session, merge_strategy: str) -> None: ...

    @classmethod
    def drop_staging_table(cls, session: so.Session) -> None: ...

    @classmethod
    def _merge_insert(cls, session: so.Session, target: str, staging: str) -> None: ...

    @classmethod
    def _merge_replace(cls, session: so.Session, target: str, staging: str, pk_cols: list[str], dialect: str) -> None: ...

    @classmethod
    def _merge_upsert(cls, session: so.Session, target: str, staging: str, pk_cols: list[str], dialect: str) -> None: ...
    
@runtime_checkable
class ParquetTableProtocol(ORMTableProtocol, Protocol):
    """
    Protocol for ORM tables that support Parquet-based ingestion.

    Normalisation and deduplication semantics are inherited
    from ORMTableProtocol / table mixins.
    """

    @classmethod
    def load_parquet(
        cls: Type["ParquetTableProtocol"],
        session: so.Session,
        path: Path,
        *,
        columns: list[str] | None = None,
        filters: list[tuple] | None = None,
        commit_on_chunk: bool = False,
    ) -> int: ...


@runtime_checkable
class SerializedTableProtocol(Protocol):
    """
    Protocol for ORM instances that can be serialized to dict / JSON
    in a stable, deterministic way.
    """

    def to_dict(
        self,
        *,
        include_nulls: bool = False,
        only: set[str] | None = None,
        exclude: set[str] | None = None,
    ) -> Dict[str, Any]: ...

    def to_json(self, **kwargs) -> str: ...

    def fingerprint(self) -> str: ...

    def __iter__(self) -> Any: ...

    def __json__(self) -> Any: ...

