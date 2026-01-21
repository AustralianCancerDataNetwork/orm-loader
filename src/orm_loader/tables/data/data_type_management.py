from typing import Any, Callable
import re, math
from datetime import datetime, date
from sqlalchemy.types import Integer, Float, Boolean, Date, DateTime, String, Text
from dateutil import parser 

_NUMERIC_RE = re.compile(r"^[+-]?\d+(\.\d+)?$")

_AVAILABLE_DATE_FORMATS = (
    "%Y%m%d",          # 20170824 (athena standard)
    "%d-%b-%Y",        # 24-AUG-2017 (oncology-branch vocab)
    "%Y-%m-%d",        # 2017-08-24 (ISO)
    "%d/%m/%Y",        # 24/08/2017 
)

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


def _parse_date(value: str) -> date | None:
    for fmt in _AVAILABLE_DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None

def _parse_datetime(value: str) -> datetime | None:
    # Try datetime first
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

def perform_cast(value: Any, col_type: Any, *, on_cast_error: Callable | None = None) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    
    # Integer
    if type(col_type) == Integer:
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    # Float
    if type(col_type) == Float:
        try:
            return float(value)
        except (ValueError, TypeError):
            if on_cast_error:
                on_cast_error(value)
            return None
    # Boolean
    if type(col_type) == Boolean:
        v = _to_bool(value)
        if v is None and on_cast_error:
            on_cast_error(value)
        return v

    # Date, DateTime
    if type(col_type) == Date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            v = _parse_date(value)
            if not v and on_cast_error:
                on_cast_error(value)
            return v
        return None
    
    if type(col_type) == DateTime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        if isinstance(value, str):
            v = _parse_datetime(value)
            if not v and on_cast_error:
                on_cast_error(value)
        return v

    # String / Text
    if isinstance(col_type, String) or isinstance(col_type, Text):
        # Canonicalise numeric-looking identifiers
        if isinstance(value, float) and value.is_integer():
            return str(int(value))

        if isinstance(value, str):
            v = value.strip()
            if v == "":
                return None
            # conservative numeric normalisation for string types - avoiding scientific notation and floating point precision issues
            v = _to_numeric_string(v) or ""
            if col_type.length and len(v) > col_type.length:
                if on_cast_error:
                    on_cast_error(value)
                v = v[: col_type.length]
            assert not col_type.length or len(v) <= col_type.length, (f"{v!r} exceeds {col_type.length} chars")
            return v

        return str(value)
    # Fallback: leave as is
    return value

def _safe_cast(value: Any, sa_type, *, on_error) -> Any:
    try:
        return perform_cast(value, sa_type, on_cast_error=on_error)
    except Exception:
        on_error(value)
        return None