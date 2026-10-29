from typing import Protocol, ClassVar, runtime_checkable, TYPE_CHECKING, Optional, Type, Dict, Any, Unpack, TypedDict
import sqlalchemy.orm as so
import sqlalchemy as sa
from collections.abc import Mapping
from pathlib import Path
from contextlib import AbstractContextManager
if TYPE_CHECKING:
    from oa_configurator import ResolvedDatabase

    from ..conversion import ConversionPolicy
    from ..loaders import FileReader, LoadReport, LoaderContext

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

    @classmethod
    def required_columns(cls) -> set[str]: ...

@runtime_checkable
class FileTableProtocol(ORMTableProtocol, Protocol):
    """
    Structural protocol for ORM tables that load from files.

    This protocol captures the expected interface for tables that can
    participate in staged, file-based loading workflows.

    It defines the contract for:
    - staging table management
    - reader selection
    - file ingestion and merging semantics

    No assumptions are made about the underlying database or schema.
    """

    @classmethod
    def _select_reader(cls, ctx: "LoaderContext") -> "FileReader": ...

    @classmethod
    def create_staging_table(cls, session: so.Session, *, staging_schema_tag: str | None = None) -> None: ...

    @classmethod
    def load_staging(cls: Type["FileTableProtocol"], loader_context: "LoaderContext", reader: "FileReader") -> int: ...

    @classmethod
    def load_file(
        cls,
        session: so.Session,
        path: Path,
        *,
        reader: Optional["FileReader"] = None,
        normalise: bool = True,
        dedupe: bool = False,
        chunksize: int | None = None,
        merge_strategy: str = "replace",
        quote_mode: str = "auto",
        index_strategy: str = "auto",
        merge_batch_size: int | None = None,
        staging_schema_tag: str | None = None,
        validate_deferred_fks: bool = True,
        resolved: Optional["ResolvedDatabase"] = None,
        physical_schemas: Optional[Mapping[str, str | None]] = None,
        policy: Optional["ConversionPolicy"] = None,
        rejects_dir: Optional[Path] = None,
        report: Optional["LoadReport"] = None,
    ) -> int: ...

    @classmethod
    def get_staging_table(cls, session: so.Session, *, staging_schema_tag: str | None = None) -> sa.Table: ...

    @classmethod
    def merge_from_staging(
        cls,
        session: so.Session,
        merge_strategy: str = "replace",
        *,
        merge_batch_size: int | None = None,
        staging_schema_tag: str | None = None,
        validate_deferred_fks: bool = True,
        resolved: Optional["ResolvedDatabase"] = None,
        physical_schemas: Optional[Mapping[str, str | None]] = None,
    ) -> None: ...

    @classmethod
    def drop_staging_table(cls, session: so.Session, *, staging_schema_tag: str | None = None) -> None: ...

    @classmethod
    def validate_foreign_keys(
        cls,
        session: so.Session,
        *,
        staging_schema_tag: str | None = None,
        resolved: Optional["ResolvedDatabase"] = None,
        physical_schemas: Optional[Mapping[str, str | None]] = None,
    ) -> None: ...

    @classmethod
    def _target_has_rows(cls, session: so.Session) -> bool: ...

    @classmethod
    def manage_indices(
        cls, session: so.Session, index_strategy: str = "auto", *, staging_schema_tag: str | None = None
    ) -> AbstractContextManager[None]:
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
