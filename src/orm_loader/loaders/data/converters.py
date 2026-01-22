import datetime
from sqlalchemy.types import Integer, Float, Boolean, Date, DateTime, String, Text
from typing import Any, Callable
import math, datetime
from dataclasses import dataclass
import pyarrow as pa
import pyarrow.compute as pc

_ARROW_TYPE_MAP = {
    Integer: pa.int64(),
    Float: pa.float64(),
    Boolean: pa.bool_(),
    Date: pa.date32(),
    DateTime: pa.timestamp("us"),
}

from .data_type_management import (
    _parse_date,
    _parse_datetime,
    _to_bool,
    _to_numeric_string,
    _cast_string
)

@dataclass(frozen=True)
class CastRule:
    sa_type: type
    scalar: Callable[[Any, Any], Any]
    arrow: Callable | None = None   # optional vectorised impl

CAST_RULES: list[CastRule] = [
    CastRule(Integer, lambda v, _: int(v) if v is not None else None),
    CastRule(Float,   lambda v, _: float(v) if v is not None else None),
    CastRule(Boolean, lambda v, _: _to_bool(v)),
    CastRule(Date,    lambda v, _: _parse_date(v)),
    CastRule(DateTime,lambda v, _: _parse_datetime(v)),
    CastRule(String,  _cast_string),
    CastRule(Text,    _cast_string),
]

def cast_scalar(value: Any, sa_type, *, on_error=None):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str) and value.strip() == "":
        return None

    for rule in CAST_RULES:
        if isinstance(sa_type, rule.sa_type):
            try:
                return rule.scalar(value, sa_type)
            except Exception:
                if on_error:
                    on_error(value)
                return None

    return value


def perform_cast(value: Any, sa_type, *, on_error) -> Any:
    return cast_scalar(value, sa_type, on_error=on_error)
    

def cast_arrow_column(arr: pa.Array, sa_col, stats=None):
    for rule in CAST_RULES:
        if isinstance(sa_col.type, rule.sa_type):
            # Use Arrow native cast if available
            arrow_type = _ARROW_TYPE_MAP.get(rule.sa_type)
            if arrow_type:
                try:
                    return pc.cast(arr, arrow_type)
                except pa.ArrowInvalid:
                    validity = pc.is_valid(arr)                     # type: ignore
                    invalid_mask = pc.invert(validity)              # type: ignore
                    invalid_count = pc.sum(invalid_mask).as_py()    # type: ignore
                    if invalid_count == 0:
                        return arr
                    
                    bad_values = [
                        v.as_py()
                        for v, bad in zip(arr, invalid_mask)
                        if bad
                    ][:3]
                    if stats:
                        stats.record(
                            column=sa_col.name, 
                            value={
                                "count": invalid_count,
                                "examples": bad_values,
                                "reason": f"Arrow cast to {arrow_type} failed"
                            },
                        )
                    return arr
            # fallback: scalar apply
            return pa.array(
                [rule.scalar(v.as_py(), sa_col) for v in arr],
                type=arr.type,
            )
    return arr