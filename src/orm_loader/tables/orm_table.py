import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Any, Tuple, Type, cast
import logging
from .allocators import IdAllocator

logger = logging.getLogger(__name__)

"""
ORMTableBase
============

Lightweight structural utilities for SQLAlchemy ORM-mapped tables.

This module provides the ORMTableBase mixin, which supplies:
- mapper access
- primary key introspection
- column inspection
- required-field detection
- safe ID allocation helpers

The functionality here is intentionally model-agnostic and contains
no domain or schema-specific logic.

"""


class ORMTableBase:
    """
    Mixin for SQLAlchemy ORM-mapped tables providing structural
    introspection and helper utilities.

    This base class intentionally avoids any domain or schema semantics
    and is designed to sit at the lowest level of a model hierarchy.

    It provides:
    - mapper access
    - primary key discovery and extraction
    - column inspection helpers
    - required-field detection for ingestion
    - monotonic ID allocation support
    """

    __abstract__ = True

    @classmethod
    def mapper_for(cls: Type) -> so.Mapper:
        """
        Return the SQLAlchemy mapper associated with this ORM class.

        This is a thin wrapper around ``sqlalchemy.inspect`` that provides
        a single, explicit access point for mapper inspection.

        Returns
        -------
        sqlalchemy.orm.Mapper
            The mapper associated with the ORM-mapped class.

        Raises
        ------
        TypeError
            If the class is not a mapped SQLAlchemy ORM class.
        """
        mapper = sa.inspect(cls)
        if not mapper:
            raise TypeError(f"{cls.__name__} is not a mapped ORM class")
        return cast(so.Mapper, mapper)

    @classmethod
    def pk_columns(cls) -> list[sa.ColumnElement]:
        """
        Return the primary key columns for the mapped table.

        The columns are returned in mapper-defined order.

        Returns
        -------
        list[sqlalchemy.ColumnElement]
            A list of primary key column objects.

        Raises
        ------
        ValueError
            If the table has no primary key defined.
        """
        pks = list(cls.mapper_for().primary_key)
        if not pks:
            raise ValueError(f"{cls.__name__} has no primary key")
        return pks

    @classmethod
    def pk_names(cls) -> list[str]:
        """
        Return the primary key column names.

        Returns
        -------
        list[str]
            A list of primary key column names.
        """
        return [c.key for c in cls.pk_columns() if c.key is not None]

    @classmethod
    def pk_values(cls, obj: Any) -> dict[str, Any]:
        """
        Extract primary key values from an ORM instance.

        Parameters
        ----------
        obj
            An ORM-mapped instance of this class.

        Returns
        -------
        dict[str, Any]
            A dictionary mapping primary key column names to values.
        """
        return {c.key: getattr(obj, c.key) for c in cls.pk_columns() if c.key is not None}
    
    @classmethod
    def pk_tuple(cls, obj: Any) -> Tuple[Any, ...]:
        """
        Extract primary key values from an ORM instance as a tuple.

        Values are returned in mapper-defined primary key order.

        Parameters
        ----------
        obj
            An ORM-mapped instance of this class.

        Returns
        -------
        tuple
            A tuple of primary key values.
        """
        return tuple(
            getattr(obj, c.key)
            for c in cls.pk_columns()
            if c.key is not None
        )

    @classmethod
    def model_columns(cls) -> dict[str, sa.ColumnElement]:
        """
        Return all mapped columns for the table.

        Returns
        -------
        dict[str, sqlalchemy.ColumnElement]
            A mapping of column name to column object.
        """
        mapper = cls.mapper_for()
        return {c.key: c for c in mapper.columns if c.key is not None}
    
    @classmethod
    def required_columns(cls) -> set[str]:
        """
        Return column names that must be present in inbound data.

        A column is considered required if it is:
        - non-nullable
        - has no Python-side default
        - has no server-side default

        This method is intended for ingestion-time validation and does
        not attempt to enforce schema semantics beyond insert viability.

        Returns
        -------
        set[str]
            A set of required column names.
        """
        mapper = cls.mapper_for()
        return {
            c.key
            for c in mapper.columns
            if not c.nullable and not c.default and not c.server_default and c.key is not None
        }

    @classmethod
    def max_id(cls, session) -> int:
        """
        Return the maximum value of the primary key column.

        This method only supports tables with a single-column primary key.

        Parameters
        ----------
        session
            A SQLAlchemy session.

        Returns
        -------
        int
            The maximum primary key value, or 0 if the table is empty.

        Raises
        ------
        ValueError
            If the table has a composite primary key.
        """
        pks = cls.pk_columns()
        if len(pks) != 1:
            raise ValueError(
                f"{cls.__name__} has composite PK; max_id() not supported"
            )
        pk = pks[0]
        return session.query(sa.func.max(pk)).scalar() or 0

    @classmethod
    def allocator(cls, session) -> IdAllocator:
        """
        Create an ID allocator initialised from the current table state.

        The allocator is initialised using the current maximum primary
        key value and can be used to generate monotonically increasing
        identifiers in environments without database-managed sequences.

        Used to support autoincrementing primary keys on dialects without
        native sequence support (e.g., SQLite).

        Parameters
        ----------
        session
            An active SQLAlchemy session.

        Returns
        -------
        IdAllocator
            An ID allocator initialised to the current maximum ID.
        """
        return IdAllocator(cls.max_id(session))
