from sqlalchemy import ColumnElement
from sqlalchemy.types import Integer, Float, Boolean, Date, DateTime, String, Text, TypeEngine
from typing import Any, Callable
from enum import Enum
import math
from dataclasses import dataclass
import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd
import re
from datetime import datetime, date
from dateutil import parser

from ..data_classes import TableCastingStats

_NUMERIC_RE = re.compile(r"^[+-]?\d+(\.\d+)?$")

_AVAILABLE_DATE_FORMATS = (
    "%Y%m%d",          # 20170824 (athena standard)
    "%d-%b-%Y",        # 24-AUG-2017 (oncology-branch vocab)
    "%Y-%m-%d",        # 2017-08-24 (ISO)
    "%d/%m/%Y",        # 24/08/2017
)


_ARROW_TYPE_MAP: dict[type, pa.DataType] = {
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
    arrow: Callable[..., Any] | None = None   # optional vectorised impl, never actually set/read today


_NULL_STRINGS = {
    "null",
    "none",
    "na",
    "n/a",
    "nan",
}

def _normalise_null(value: Any) -> Any | None:
    if value is None:
        return None
    
    if value is pd.NA or value is pd.NaT:
        return None

    if isinstance(value, float) and math.isnan(value):
        return None

    if isinstance(value, str):
        s = value.strip().lower()
        if s == "":
            return None
        if s in _NULL_STRINGS:
            return None

    return value


_ARROW_NULL_STRINGS = pa.array(sorted(_NULL_STRINGS | {""}), type=pa.string())


def _normalise_null_arrow(arr: pa.Array | pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
    arrow_type = arr.type

    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        probe = pc.utf8_lower(pc.utf8_trim_whitespace(arr))                 # type: ignore
        return pc.if_else(                                                  # type: ignore
            pc.is_in(probe, value_set=_ARROW_NULL_STRINGS),                 # type: ignore
            pa.nulls(len(arr), arrow_type),
            arr,
        )

    if pa.types.is_floating(arrow_type):
        return pc.if_else(                                                  # type: ignore
            pc.is_nan(arr),                                                 # type: ignore
            pa.nulls(len(arr), arrow_type),
            arr,
        )

    return arr


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

def _cast_string(value: Any, sa_type: TypeEngine[Any]) -> str | None:
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

def _to_float(value: Any) -> float | None:
    if value is None:
        return None

    if isinstance(value, float):
        if math.isnan(value):
            return None
        return float(value)

    if isinstance(value, int):
        return float(value)

    if isinstance(value, str):
        s = value.strip()
        if s == "":
            return None
        try:
            return float(s)
        except ValueError:
            return None

    return None


CAST_RULES: list[CastRule] = [
    CastRule(Integer, lambda v, _: _to_int(v) if v is not None else None),
    CastRule(Float,   lambda v, _: _to_float(v) if v is not None else None),
    CastRule(Boolean, lambda v, _: _to_bool(v)),
    CastRule(Date,    lambda v, _: _parse_date(v)),
    CastRule(DateTime,lambda v, _: _parse_datetime(v)),
    CastRule(String,  _cast_string),
    CastRule(Text,    _cast_string),
]

# Per-column overrides, keyed by (table_name, column_name). Checked ahead of
# CAST_RULES, since dispatch there is purely by SQLAlchemy type and can't
# target one specific column (e.g. a plain String(1) column with no
# type-level signal of the enum it's meant to hold) or safely disambiguate
# sa.Enum from its own String supertype without a caller telling it which
# column actually means "enum" 
_COLUMN_CAST_RULES: dict[tuple[str, str], Callable[[Any], Any]] = {}


def _enum_member_scalar(enum_type: type[Enum]) -> Callable[[Any], Any]:
    # Built once per registration rather than per row: every CSV load reads with
    # dtype=str, so an int-valued member arrives as "1" -- which neither
    # enum_type(value) nor enum_type(str(value)) can match.
    _by_str_value = {str(member.value).strip(): member.name for member in enum_type}

    def _scalar(value: Any) -> Any:
        if value is None:
            return None

        # Match by .value (the raw display text a real data source uses),
        # store by .name -- sa.Enum's own default column-storage convention.
        # The staging-to-target merge is a plain SQL copy with no Python-level
        # type translation, so this must already be exactly what the column
        # expects to find; .name is that default for an unmodified sa.Enum.

        # Already the right member: only reachable via direct cast_scalar use,
        # since file sources yield raw scalars. Handled ahead of the str()
        # fallback, which would mangle it to "SomeEnum.MEMBER".
        if isinstance(value, enum_type):
            return value.name

        # Raw value first, so enums whose member values aren't strings
        # (IntEnum and friends -- sa.Enum still persists .name for those) match
        # without a lossy str() round-trip.
        try:
            return enum_type(value).name
        except (ValueError, KeyError, TypeError):
            pass

        s = str(value).strip()
        if s == "":
            return None
        try:
            return enum_type(s).name
        except (ValueError, KeyError, TypeError):
            pass

        try:
            return _by_str_value[s]
        except KeyError:
            # Raise rather than return None, so an unmatched value goes through
            # the on_error/TableCastingStats path like every other cast failure.
            raise ValueError(f"{value!r} matches no {enum_type.__name__} member value")
    return _scalar


def register_column_cast_rule(
    table_name: str,
    column_name: str,
    scalar: Callable[[Any], Any] | None = None,
    *,
    enum_type: type[Enum] | None = None,
) -> None:
    """
    Register custom cast/validation logic for one specific column.

    If `enum_type` is given, the raw value is matched against its members
    by `.value` and coerced to the matching member's `.name`. Member values
    need not be strings -- an `IntEnum` matches both `1` (a parquet int64
    column) and `"1"` (any CSV column, since those are read with dtype=str).
    
    This is `sa.Enum`'s default column-storage convention, and covers genuine
    `sa.Enum(enum_type)` columns with no further setup. An unmatched value
    goes through the existing on_error/TableCastingStats path and the
    column is set to None, same as every other cast failure today.

    For anything that departs from that default convention (a plain
    String/Char column with an implied enum and no `sa.Enum` storage
    convention at all (e.g. OMOP CDM's `concept.standard_concept`:
    `String(1)`, values 'S'/'C'/NULL, where the column expects `.value`
    directly, not `.name`), or a genuine `sa.Enum` column configured with a
    custom `values_callable` such as in HemOnc) use `scalar` instead. 
    Resolution logic for the scalar callable is then tailored to what 
    that specific column actually expects to store (e.g.
    `lambda v: SomeEnum(v).value` for the plain-String case above).
    
    Registering the same (table_name, column_name) again replaces the
    previous rule rather than stacking, so repeated registration is
    safe/idempotent.
    """
    if enum_type is not None:
        if scalar is not None:
            raise ValueError("register_column_cast_rule requires exactly one of `scalar` or `enum_type`.")
        resolved_scalar = _enum_member_scalar(enum_type)
    elif scalar is not None:
        resolved_scalar = scalar
    else:
        raise ValueError("register_column_cast_rule requires exactly one of `scalar` or `enum_type`.")

    _COLUMN_CAST_RULES[(table_name, column_name)] = resolved_scalar

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

def cast_scalar(
    value: Any,
    sa_type: TypeEngine[Any],
    *,
    on_error: Callable[[Any], None] | None = None,
    table_name: str | None = None,
    column_name: str | None = None,
) -> Any:
    value = _normalise_null(value)
    if value is None:
        return None

    column_rule = _COLUMN_CAST_RULES.get((table_name, column_name)) if table_name and column_name else None
    if column_rule is not None:
        try:
            return column_rule(value)
        except Exception:
            if on_error:
                on_error(value)
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


def perform_cast(
    value: Any,
    sa_type: TypeEngine[Any],
    *,
    on_error: Callable[[Any], None] | None,
    table_name: str | None = None,
    column_name: str | None = None,
) -> Any:
    return cast_scalar(value, sa_type, on_error=on_error, table_name=table_name, column_name=column_name)


def cast_arrow_column(arr: pa.Array, sa_col: ColumnElement[Any], stats: TableCastingStats | None = None) -> pa.Array:
    arr = _normalise_null_arrow(arr)

    column_rule = _COLUMN_CAST_RULES.get((sa_col.table.name, sa_col.name))
    if column_rule is not None:
        values: list[Any] = []
        for v in arr:
            raw = v.as_py()
            try:
                values.append(column_rule(raw) if raw is not None else None)
            except Exception:
                if stats:
                    stats.record(column=sa_col.name, value=raw)
                values.append(None)
        return pa.array(values, type=arr.type)

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