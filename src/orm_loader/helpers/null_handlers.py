from typing import Any

import pandas as pd

_NULL_STRINGS = {"", "nan", "null", "none", "na", "n/a"}


def normalise_null(value: Any) -> Any | None:
    if value is None:
        return None

    # pandas / numpy NaN. Only scalars are nullable; array-likes are returned unchanged.
    if pd.api.types.is_scalar(value) and pd.isna(value):
        return None

    # string garbage
    if isinstance(value, str):
        s = value.strip().lower()
        if s in _NULL_STRINGS:
            return None
        return value  # keep legit strings

    return value
