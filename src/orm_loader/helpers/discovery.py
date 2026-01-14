from typing import Type
from .metadata import Base

def get_model_by_tablename(tablename: str, base: Type[Base] | None = None) -> Type | None:
    tablename = tablename.lower().strip()
    if base is None:
        base = Base
    for cls in base.__subclasses__():
        if getattr(cls, "__tablename__", None) == tablename:
            return cls
    return None
