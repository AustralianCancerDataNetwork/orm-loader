from typing import Any
import re
import math
from datetime import datetime, date
from dateutil import parser 
from sqlalchemy.types import String, Text

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