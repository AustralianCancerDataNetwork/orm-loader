from .orm_table import ORMTableBase
from typing import Any
import json
import hashlib
import datetime

def json_default(obj) -> str:
    """
    Default JSON serialisation handler for unsupported types.

    Currently supports ISO-8601 serialisation for ``datetime.date``
    and ``datetime.datetime`` objects.

    Parameters
    ----------
    obj
        The object to serialise.

    Returns
    -------
    str
        A JSON-serialisable representation of the object.

    Raises
    ------
    TypeError
        If the object type is not supported.
    """
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serialisable")

    
class SerialisableTableInterface(ORMTableBase):
    """
    Mixin for SQLAlchemy ORM tables providing explicit serialisation helpers.

    This interface adds lightweight, deterministic helpers for converting
    ORM-mapped rows into dictionaries, JSON strings, and stable fingerprints.

    It is intended for:
    - debugging and inspection
    - auditing and reproducibility checks
    - lightweight export or API layers
    - content-addressable comparisons

    No assumptions are made about schema semantics or domain logic.
    """

    __abstract__ = True

    def to_dict(
        self,
        *,
        include_nulls: bool = False,
        only: set[str] | None = None,
        exclude: set[str] | None = None,
    ) -> dict[str, Any]:
        
        """
        Convert the ORM instance into a dictionary.

        The output is derived directly from the mapped model columns
        and reflects the current in-memory state of the object.

        Parameters
        ----------
        include_nulls
            Whether to include keys whose values are ``None``.
            Defaults to ``False``.
        only
            An optional set of column names to include.
            If provided, all other columns are ignored.
        exclude
            An optional set of column names to exclude.

        Returns
        -------
        dict[str, Any]
            A dictionary representation of the ORM row.
        """
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
        """
        Serialise the ORM instance to a JSON string.

        This method delegates to :meth:`to_dict` and applies a stable,
        deterministic JSON encoding with sorted keys.

        Parameters
        ----------
        **kwargs
            Keyword arguments forwarded to :meth:`to_dict`.

        Returns
        -------
        str
            A JSON representation of the ORM row.
        """
        return json.dumps(
            self.to_dict(**kwargs),
            default=json_default,
            sort_keys=True,
        )
    
    def fingerprint(self) -> str:
        """
        Compute a stable fingerprint for the ORM instance.

        The fingerprint is derived from the JSON serialisation of the
        row with null values included and is suitable for:
        - change detection
        - deduplication
        - caching
        - reproducibility checks

        Returns
        -------
        str
            A SHA-256 hexadecimal digest representing the row content.
        """
        payload = self.to_json(include_nulls=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    
    def __iter__(self):
        """
        Iterate over the ORM instance as ``(key, value)`` pairs.

        This enables lightweight unpacking and interoperability with
        dictionary-based APIs.

        Yields
        ------
        tuple[str, Any]
            Column name and value pairs.
        """
        yield from self.to_dict().items()

    def __json__(self):
        """
        Return a JSON-serialisable representation of the ORM instance.

        This hook is provided for compatibility with JSON encoders
        that check for a ``__json__`` method.

        Returns
        -------
        dict[str, Any]
            A dictionary representation of the ORM row.
        """
        return self.to_dict()