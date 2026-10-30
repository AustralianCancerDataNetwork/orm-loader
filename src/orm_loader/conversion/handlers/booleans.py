"""Handler for boolean columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy.types import TypeEngine

from ..values import Interpretation
from .base import ConvertingHandler


@dataclass(frozen=True)
class BooleanHandler(ConvertingHandler):
    """Reads case-insensitive words as true or false.

    Attributes
    ----------
    true_words
        Lowercase words read as true.
    false_words
        Lowercase words read as false.
    """

    true_words: tuple[str, ...] = ("true", "t", "yes", "y", "1", "on")
    false_words: tuple[str, ...] = ("false", "f", "no", "n", "0", "off")

    def arrow_type(self, sa_type: TypeEngine[Any]) -> pa.DataType:
        return pa.bool_()

    def parse(self, cleaned: pa.Array, sa_type: TypeEngine[Any]) -> Interpretation:
        lowered = pc.utf8_lower(cleaned)
        is_true = pc.fill_null(pc.is_in(lowered, value_set=pa.array(self.true_words, pa.string())), False)
        is_false = pc.fill_null(pc.is_in(lowered, value_set=pa.array(self.false_words, pa.string())), False)
        known = pc.or_(is_true, is_false)
        values = pc.if_else(known, is_true, pa.scalar(None, pa.bool_()))
        return Interpretation(values, pc.and_(pc.is_valid(cleaned), pc.invert(known)))
