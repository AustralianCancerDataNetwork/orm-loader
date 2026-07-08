from typing import Protocol, ClassVar, runtime_checkable, TYPE_CHECKING, Optional, Type, Dict, Any, Unpack, TypedDict
import sqlalchemy.orm as so
import sqlalchemy as sa
from pathlib import Path
from contextlib import AbstractContextManager
if TYPE_CHECKING:
    from ..loaders import LoaderContext, LoaderInterface

class ToDictKwargs(TypedDict, total=False):
    include_nulls: bool
    only: set[str] | None
    exclude: set[str] | None

@runtime_checkable
class ORMTableProtocol(Protocol):
    """
    Structural protocol for ORM-mapped *table classes*.

    This protocol defines the minimal structural surface expected of
    SQLAlchemy ORM table classes within ``orm-loader``.

    It is intentionally lightweight and model-agnostic, focusing on:
    - core SQLAlchemy table attributes
    - primary key introspection
    - column discovery helpers

    This protocol is used for static typing, runtime checks, and to
    decouple higher-level infrastructure from concrete base classes.
    """

    __tablename__: ClassVar[str]
    __table__: ClassVar[sa.Table]
    metadata: ClassVar[sa.MetaData]

    @classmethod
    def mapper_for(cls) -> so.Mapper[Any]: ...

    @classmethod
    def pk_names(cls) -> list[str]: ...

    @classmethod
    def pk_columns(cls) -> list[sa.ColumnElement[Any]]: ...

    @classmethod
    def model_columns(cls) -> dict[str, sa.ColumnElement[Any]]: ...

@runtime_checkable
class CSVTableProtocol(ORMTableProtocol, Protocol):
    """
    Structural protocol for ORM tables that support CSV-based ingestion.

    This protocol captures the expected interface for tables that can
    participate in staged, file-based loading workflows using CSV input.

    It defines the contract for:
    - staging table management
    - loader selection
    - CSV ingestion and merging semantics

    No assumptions are made about the underlying database or schema.
    """

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
        loader: Optional["LoaderInterface"] = None,
        normalise: bool = True,
        dedupe: bool = False,
        chunksize: int | None = None,
        merge_strategy: str = "replace",
        quote_mode: str = "csv",
        index_strategy: str = "auto",
        merge_batch_size: int | None = None,
    ) -> int: ...

    @classmethod
    def orm_staging_load(cls, loader: "LoaderInterface", loader_context: "LoaderContext") -> int: ...

    @classmethod
    def get_staging_table(cls, session: so.Session) -> sa.Table: ...

    @classmethod
    def merge_from_staging(
        cls,
        session: so.Session,
        merge_strategy: str = "replace",
        *,
        merge_batch_size: int | None = None,
    ) -> None: ...

    @classmethod
    def drop_staging_table(cls, session: so.Session) -> None: ...

    @classmethod
    def _target_has_rows(cls, session: so.Session, target: str) -> bool: ...

    @classmethod
    def manage_indices(cls, session: so.Session, index_strategy: str = "auto") -> AbstractContextManager[None]:
        ...
    

@runtime_checkable
class SerializedTableProtocol(Protocol):
    """
    Structural protocol for ORM instances that support stable serialisation.

    This protocol defines the expected interface for ORM row instances
    that can be converted to dictionaries, JSON strings, and deterministic
    fingerprints.

    It is primarily used for typing and interoperability with downstream
    consumers such as validators, APIs, and audit tooling.
    """

    def to_dict(
        self,
        *,
        include_nulls: bool = False,
        only: set[str] | None = None,
        exclude: set[str] | None = None,
    ) -> Dict[str, Any]: ...

    def to_json(self, **kwargs: Unpack[ToDictKwargs]) -> str: ...

    def fingerprint(self) -> str: ...

    def __iter__(self) -> Any: ...

    def __json__(self) -> Any: ...
