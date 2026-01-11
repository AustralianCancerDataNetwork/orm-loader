from typing import Type
from .metadata import Base

def get_model_by_tablename(tablename: str) -> Type | None:
    tablename = tablename.lower().strip()
    for cls in Base.__subclasses__():
        if getattr(cls, "__tablename__", None) == tablename:
            return cls
    return None
