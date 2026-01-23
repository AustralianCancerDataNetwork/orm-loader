from sqlalchemy.types import Integer, Float, Boolean, Date, DateTime, String, Text
from typing import Any, Callable
import math
from dataclasses import dataclass
import pyarrow as pa
import pyarrow.compute as pc

import re
from datetime import datetime, date
from dateutil import parser 

_NUMERIC_RE = re.compile(r"^[+-]?\d+(\.\d+)?$")

_AVAILABLE_DATE_FORMATS = (
    "%Y%m%d",          # 20170824 (athena standard)
    "%d-%b-%Y",        # 24-AUG-2017 (oncology-branch vocab)
    "%Y-%m-%d",        # 2017-08-24 (ISO)
    "%d/%m/%Y",        # 24/08/2017 
)


_ARROW_TYPE_MAP = {
    Integer: pa.int64(),
    Float: pa.float64(),
    Boolean: pa.bool_(),
    Date: pa.date32(),
    DateTime: pa.timestamp("us"),
}

@dataclass(frozen=True)
class CastRule:
    sa_type: type
    scalar: Callable[[Any, Any], Any]
    arrow: Callable | None = None   # optional vectorised impl


def _to_numeric_string(value: str | None) -> str | None:
    if value is None:
        return None

    if not _NUMERIC_RE.match(value):
        return value  

    if "." in value:
        f = float(value)
        if f.is_integer():
            return str(int(f))
        return str(f)

    return str(int(value))

def _to_number(value: Any) -> int | float | None:
    if value is None:
        return None

    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return int(value)
        raise ValueError(f"Non-integer float: {value}")

    if isinstance(value, int):
        return value

    if isinstance(value, str):
        s = value.strip()
        if s == "":
            return None

        s = _to_numeric_string(s)
        if s is None:
            return None
        
        return int(s)


def _to_int(value: Any) -> int | None:
    n = _to_number(value)
    if n is None:
        return None

    if isinstance(n, int):
        return n

    raise ValueError(f"Non-integer numeric value: {value}")

def _cast_string(value: Any, sa_type) -> str | None:
    if value is None:
        return None

    if isinstance(value, float) and math.isnan(value):
        return None

    s = str(value).strip()
    if s == "":
        return None

    s = _to_numeric_string(s) or ""

    if isinstance(sa_type, (String, Text)) and sa_type.length:
        if len(s) > sa_type.length:
            return s[: sa_type.length]

    return s

CAST_RULES: list[CastRule] = [
    CastRule(Integer, lambda v, _: _to_int(v) if v is not None else None),
    CastRule(Float,   lambda v, _: _to_number(v) if v is not None else None),
    CastRule(Boolean, lambda v, _: _to_bool(v)),
    CastRule(Date,    lambda v, _: _parse_date(v)),
    CastRule(DateTime,lambda v, _: _parse_datetime(v)),
    CastRule(String,  _cast_string),
    CastRule(Text,    _cast_string),
]

def _dateutil_fallback(value: str) -> datetime | None:
    try:
        dt = parser.parse(
            value,
            dayfirst=True,
            yearfirst=False,
            fuzzy=False,  
        )
    except (ValueError, OverflowError):
        return None

    normalised = dt.strftime("%Y-%m-%d")
    if normalised not in value:
        return None

    return dt

def _parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        for fmt in _AVAILABLE_DATE_FORMATS:
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        return None

def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass

    # Fallback to date-only formats + midnight
    d = _parse_date(value)
    if d:
        return datetime.combine(d, datetime.min.time())

    return _dateutil_fallback(value)

def _to_bool(value: Any) -> bool | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"true", "t", "yes", "y", "1"}:
        return True
    if s in {"false", "f", "no", "n", "0"}:
        return False
    return None

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