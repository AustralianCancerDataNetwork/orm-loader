from .orm_table import ORMTableBase
from typing import Any
import json, hashlib, datetime
from ..data.converters import json_default
    
class SerialisableTableInterface(ORMTableBase):
    """
    Mixin for ORM tables that can be serialized to/from dicts/JSON.
    """

    __abstract__ = True

    def to_dict(
        self,
        *,
        include_nulls: bool = False,
        only: set[str] | None = None,
        exclude: set[str] | None = None,
    ) -> dict[str, Any]:
        
        data = {}
        for key, _ in self.model_columns().items():
            if only and key not in only:
                continue
            if exclude and key in exclude:
                continue
            value = getattr(self, key)
            if value is None and not include_nulls:
                continue
            data[key] = value
        return data

    def to_json(self, **kwargs) -> str:
        return json.dumps(
            self.to_dict(**kwargs),
            default=json_default,
            sort_keys=True,
        )
    
    def fingerprint(self) -> str:
        payload = self.to_json(include_nulls=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    
    def __iter__(self):
        yield from self.to_dict().items()

    def __json__(self):
        return self.to_dict()