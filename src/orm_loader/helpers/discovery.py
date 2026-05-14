from typing import TypeVar
from .metadata import Base

ModelT = TypeVar("ModelT", bound=Base)

def get_model_by_tablename(
    tablename: str,
    base: type[ModelT] = Base,
) -> type[ModelT] | None:
    tablename = tablename.lower().strip()
    for cls in base.__subclasses__():
        if getattr(cls, "__tablename__", None) == tablename:
            return cls
    return None
