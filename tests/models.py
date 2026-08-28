
from enum import Enum

import sqlalchemy as sa
from sqlalchemy.orm import declarative_base
import sqlalchemy.orm as so
from orm_loader.tables import CSVLoadableTableInterface

Base = declarative_base()


class Role(str, Enum):
    FIRST_AUTHOR = "first author"
    LAST_AUTHOR = "last author"


class Flag(str, Enum):
    STANDARD = "S"
    CLASSIFICATION = "C"

class PandasLoaderTable(CSVLoadableTableInterface, Base):
    __tablename__ = "test_pandas_loader"
    id = sa.Column(sa.Integer, primary_key=True)
    value = sa.Column(sa.String, nullable=False)


class SimpleTable(Base, CSVLoadableTableInterface):
    __tablename__ = "test_table"
    __table_args__ = (
        sa.Index("ix_test_table_name", "name"),
    )

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


class RequiredTable(Base, CSVLoadableTableInterface):
    __tablename__ = "required_table"

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


class CompositeTable(Base, CSVLoadableTableInterface):
    __tablename__ = "composite_table"

    a: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    b: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    value: so.Mapped[str] = so.mapped_column(sa.String)


class EnumTable(Base, CSVLoadableTableInterface):
    """A genuine sa.Enum column, unmodified -- register_column_cast_rule's
    enum_type matches a raw value (e.g. "first author") by .value but stores
    the matching member's .name, which is exactly sa.Enum's own default
    column-storage convention -- no values_callable or other customisation
    needed for this, the common case.
    """

    __tablename__ = "enum_table"

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    role: so.Mapped[Role | None] = so.mapped_column(sa.Enum(Role), nullable=True)


class ComputedColumnTable(Base, CSVLoadableTableInterface):
    """A real, registered table with a computed column, for merge-method
    tests that need get_staging_table() to work. Unlike a bare
    __tablename__/__table__ pair, this actually implements
    CSVLoadableTableInterface."""

    __tablename__ = "computed_column_table"

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String)
    slug: so.Mapped[str] = so.mapped_column(sa.String, sa.Computed("lower(name)"))


class ImpliedEnumTable(Base, CSVLoadableTableInterface):
    """A plain String column with no type-level enum signal at all -- the
    OMOP CDM concept.standard_concept/invalid_reason shape register_column_cast_rule
    exists to cover, since sa.Enum-keyed dispatch can never reach a column like this.
    """

    __tablename__ = "implied_enum_table"

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    flag: so.Mapped[str | None] = so.mapped_column(sa.String(1), nullable=True)
