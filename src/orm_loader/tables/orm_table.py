import logging
from typing import Any

import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.exc import NoInspectionAvailable

from .allocators import IdAllocator

logger = logging.getLogger(__name__)


def _resolves_to_autoincrement(c: sa.Column) -> bool:
    """Whether a column will actually autoincrement, resolving SQLAlchemy's
    own default ``autoincrement="auto"`` sentinel the same way SQLAlchemy
    itself does at DDL-emission time.

    SQLAlchemy restricts real autoincrement behaviour to a single-column 
    integer primary key (or an explicit ``autoincrement=True``). 
    Treating every `"auto"` column as autoincrementing would wrongly 
    exclude genuinely-required string PKs from `required_columns()`.
    """
    if c.autoincrement is False:
        return False
    if c.autoincrement is True:
        return True
    if not c.primary_key:
        return False
    pk_columns = list(c.table.primary_key.columns)
    return len(pk_columns) == 1 and pk_columns[0] is c and isinstance(c.type, sa.Integer)

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
    def mapper_for(cls: type[Any]) -> so.Mapper[Any]:
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
        try:
            return sa.inspect(cls)
        except NoInspectionAvailable:
            raise TypeError(f"{cls.__name__} is not a mapped ORM class")

    @classmethod
    def pk_columns(cls) -> list[sa.ColumnElement[Any]]:
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
    def model_columns(cls) -> dict[str, sa.ColumnElement[Any]]:
        """
        Return all mapped columns for the table.

        Returns
        -------
        dict[str, sqlalchemy.ColumnElement]
            A mapping of column name to column object.
        """
        mapper = cls.mapper_for()
        return {c.key: c for c in mapper.columns}
    
    @classmethod
    def required_columns(cls) -> set[str]:
        """
        Return column names that must be present in inbound data.

        A column is considered required if it is:
        - non-nullable
        - has no default that SQL can apply (see ``sql_defaults``)
        - not an autoincrementing column (the database assigns its value;
          it's never expected in inbound data at all)

        This method is intended for ingestion-time validation and does
        not attempt to enforce schema semantics beyond insert viability.

        Returns
        -------
        set[str]
            A set of required column names.
        """
        mapper = cls.mapper_for()
        defaults = cls.sql_defaults()
        return {
            c.key
            for c in mapper.columns
            if not c.nullable and c.key not in defaults and not _resolves_to_autoincrement(c)
        }

    @classmethod
    def sql_defaults(cls) -> dict[str, sa.ColumnElement[Any]]:
        """
        Return the default of each column that has one SQL can apply.

        A server default given as text is written inline, as in the DDL, so
        the database reads it as the column's type. One given as a SQL
        expression is used as written, and a scalar Python default is a typed
        literal. A callable or context-sensitive default has no SQL form, so
        its column is not listed.

        Returns
        -------
        dict[str, sqlalchemy.ColumnElement]
            A mapping of column name to the expression that supplies its default.
        """
        defaults: dict[str, sa.ColumnElement[Any]] = {}
        for c in cls.mapper_for().columns:
            server_default = getattr(c.server_default, "arg", None)
            if isinstance(server_default, str):
                defaults[c.name] = sa.literal(server_default, sa.String()).render_literal_execute()
            elif server_default is not None:
                defaults[c.name] = server_default
            elif c.default is not None and c.default.is_scalar:
                defaults[c.name] = sa.literal(c.default.arg, c.type)  # ty: ignore[unresolved-attribute]
        return defaults

    @classmethod
    def max_id(cls, session: so.Session) -> int:
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
    def allocator(cls, session: so.Session) -> IdAllocator:
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


