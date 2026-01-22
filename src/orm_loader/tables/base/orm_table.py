import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Any, Tuple, Type, cast
import logging
from .allocators import IdAllocator

logger = logging.getLogger(__name__)

class ORMTableBase:
    """
    Mixin for SQLAlchemy ORM-mapped tables providing convenience methods for:

    - primary key introspection
    - ID allocation helpers
    - mapper access
    """

    __abstract__ = True

    @classmethod
    def mapper_for(cls: Type) -> so.Mapper:
        mapper = sa.inspect(cls)
        if not mapper:
            raise TypeError(f"{cls.__name__} is not a mapped ORM class")
        return cast(so.Mapper, mapper)

    @classmethod
    def pk_columns(cls) -> list[sa.ColumnElement]:
        pks = list(cls.mapper_for().primary_key)
        if not pks:
            raise ValueError(f"{cls.__name__} has no primary key")
        return pks

    @classmethod
    def pk_names(cls) -> list[str]:
        return [c.key for c in cls.pk_columns() if c.key is not None]

    @classmethod
    def pk_values(cls, obj: Any) -> dict[str, Any]:
        return {c.key: getattr(obj, c.key) for c in cls.pk_columns() if c.key is not None}
    
    @classmethod
    def pk_tuple(cls, obj: Any) -> Tuple[Any, ...]:
        return tuple(getattr(obj, c.key) for c in cls.pk_columns() if c.key is not None)

    @classmethod
    def model_columns(cls) -> dict[str, sa.ColumnElement]:
        mapper = cls.mapper_for()
        mc = {c.key: c for c in mapper.columns if c.key is not None}
        for name, sa_col in mc.items():
            print(f"{name}: {sa_col} ({type(sa_col)})")

            assert isinstance(sa_col, sa.Column), f"Unexpected column type: {type(sa_col)}"

        return mc
    
    @classmethod
    def required_columns(cls) -> set[str]:
        """
        Columns that must be present in inbound data for insert to succeed,
        excluding those with defaults or server defaults.
        """
        mapper = cls.mapper_for()
        return {
            c.key
            for c in mapper.columns
            if not c.nullable and not c.default and not c.server_default and c.key is not None
        }

    @classmethod
    def max_id(cls, session) -> int:
        pks = cls.pk_columns()
        if len(pks) != 1:
            raise ValueError(
                f"{cls.__name__} has composite PK; max_id() not supported"
            )
        pk = pks[0]
        return session.query(sa.func.max(pk)).scalar() or 0

    @classmethod
    def allocator(cls, session) -> IdAllocator:
        return IdAllocator(cls.max_id(session))
