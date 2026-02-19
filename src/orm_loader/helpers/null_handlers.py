import math
import pandas as pd
from typing import Any

_NULL_STRINGS = {"", "nan", "null", "none", "na", "n/a"}

def normalise_null(value: Any) -> Any | None:
    if value is None:
        return None

    # pandas / numpy NaN
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    # string garbage
    if isinstance(value, str):
        s = value.strip().lower()
        if s in _NULL_STRINGS:
            return None
        return value  # keep legit strings

    return value
