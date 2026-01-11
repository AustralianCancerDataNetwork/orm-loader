from typing import Protocol, ClassVar, runtime_checkable, TYPE_CHECKING, Optional, Type, Dict, Any
import sqlalchemy.orm as so
import sqlalchemy as sa
import pandas as pd
from pathlib import Path

@runtime_checkable
class ORMTableProtocol(Protocol):
    
    """
    Structural protocol for ORM-mapped *table classes*.
    """

    __tablename__: ClassVar[str]

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
    
    @classmethod
    def normalise_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame: ...

    @classmethod
    def dedupe_dataframe(cls, df: pd.DataFrame, *, session: so.Session | None = None, max_bind_vars: int = 10_000) -> pd.DataFrame: ...
    

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
        commit_per_chunk: bool = False,
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

