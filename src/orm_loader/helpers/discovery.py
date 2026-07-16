from typing import TypeVar
from .metadata import Base

ModelT = TypeVar("ModelT", bound=Base)

def get_model_by_tablename(
    tablename: str,
    base: type[ModelT] = Base,  # ty: ignore[invalid-parameter-default]
) -> type[ModelT] | None:
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
