from typing import overload

from .metadata import Base


@overload
def get_model_by_tablename(tablename: str) -> type[Base] | None: ...


@overload
def get_model_by_tablename[ModelT: Base](
    tablename: str, base: type[ModelT]
) -> type[ModelT] | None: ...


def get_model_by_tablename(tablename: str, base: type[Base] = Base) -> type[Base] | None:
    tablename = tablename.lower().strip()
    for mapper in base.registry.mappers:
        cls = mapper.class_
        if not isinstance(cls, type):
            continue
        if not issubclass(cls, base):
            continue
        if getattr(cls, "__tablename__", None) == tablename:
            return cls
    return None
