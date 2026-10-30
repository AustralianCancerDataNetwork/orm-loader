
from datetime import date, datetime
from decimal import Decimal
from enum import Enum

import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import Role as SchemaRole
from sqlalchemy.orm import declarative_base

from orm_loader.tables import FileLoadableTableInterface

Base = declarative_base()


class Role(str, Enum):
    FIRST_AUTHOR = "first author"
    LAST_AUTHOR = "last author"



class FileLoaderTable(FileLoadableTableInterface, Base):
    __tablename__ = "test_file_loader"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}
    id = sa.Column(sa.Integer, primary_key=True)
    value = sa.Column(sa.String, nullable=False)


class SimpleTable(Base, FileLoadableTableInterface):
    __tablename__ = "test_table"
    __table_args__ = (
        sa.Index("ix_test_table_name", "name"),
        {"schema": SchemaRole.PRIMARY.value},
    )

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


class RequiredTable(Base, FileLoadableTableInterface):
    __tablename__ = "required_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


class CompositeTable(Base, FileLoadableTableInterface):
    __tablename__ = "composite_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    a: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    b: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    value: so.Mapped[str] = so.mapped_column(sa.String)


class CycleATable(Base, FileLoadableTableInterface):
    """Mutually-referencing pair with CycleBTable, for deferred-FK tests:
    DuckDB can't execute the ALTER TABLE ADD CONSTRAINT this cycle forces."""

    __tablename__ = "cycle_a_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    b_id: so.Mapped[int | None] = so.mapped_column(
        sa.Integer, sa.ForeignKey(f"{SchemaRole.PRIMARY.value}.cycle_b_table.id")
    )


class CycleBTable(Base, FileLoadableTableInterface):
    __tablename__ = "cycle_b_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    a_id: so.Mapped[int | None] = so.mapped_column(
        sa.Integer, sa.ForeignKey(f"{SchemaRole.PRIMARY.value}.cycle_a_table.id")
    )


class EnumTable(Base, FileLoadableTableInterface):
    """A genuine sa.Enum column. Files hold the member names, which is what
    sa.Enum stores by default."""

    __tablename__ = "enum_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    role: so.Mapped[Role | None] = so.mapped_column(sa.Enum(Role), nullable=True)


class ComputedColumnTable(Base, FileLoadableTableInterface):
    """A real, registered table with a computed column, for merge-method
    tests that need a staging table to be reflected. Unlike a bare
    __tablename__/__table__ pair, this actually implements
    FileLoadableTableInterface."""

    __tablename__ = "computed_column_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String)
    # nullable: DuckDB doesn't support any constraint (including NOT NULL) on a generated column.
    slug: so.Mapped[str | None] = so.mapped_column(sa.String, sa.Computed("lower(name)"))


class ReferencedParentTable(Base, FileLoadableTableInterface):
    """Referenced by ReferencingChildTable *and* owns its own FK, for
    merge_replace's up-front DuckDB guard: a table in this shape can't be
    SET on its own FK column while still referenced, even with indexes
    dropped."""

    __tablename__ = "referenced_parent_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    grandparent_id: so.Mapped[int] = so.mapped_column(
        sa.Integer, sa.ForeignKey(f"{SchemaRole.PRIMARY.value}.unreferenced_grandparent_table.id")
    )
    name: so.Mapped[str] = so.mapped_column(sa.String)


class UnreferencedGrandparentTable(Base, FileLoadableTableInterface):
    __tablename__ = "unreferenced_grandparent_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String)


class ReferencingChildTable(Base, FileLoadableTableInterface):
    __tablename__ = "referencing_child_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    parent_id: so.Mapped[int] = so.mapped_column(
        sa.Integer, sa.ForeignKey(f"{SchemaRole.PRIMARY.value}.referenced_parent_table.id")
    )


class VocabSchemaTable(Base, FileLoadableTableInterface):
    """A VOCAB-tagged table, so tests can prove the staging/index schema
    derivation actually resolves a non-primary schema tag correctly."""

    __tablename__ = "test_vocab_role_table"
    __table_args__ = (
        sa.Index("ix_test_vocab_role_table_name", "name"),
        {"schema": SchemaRole.VOCAB.value},
    )

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


class VocabReferencingTable(Base, FileLoadableTableInterface):
    """PRIMARY-tagged table with a foreign key into VocabSchemaTable (VOCAB),
    for cross-schema foreign-key classification tests: DuckDB cannot
    enforce a key between two physical schemas, so oa-configurator omits
    it from DDL, same practical outcome as a cyclic/deferred key but
    reached a different way."""

    __tablename__ = "vocab_referencing_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    vocab_id: so.Mapped[int | None] = so.mapped_column(
        sa.Integer, sa.ForeignKey(f"{SchemaRole.VOCAB.value}.test_vocab_role_table.id")
    )


class SelfReferentialTable(Base, FileLoadableTableInterface):
    """A table referencing its own rows, for dependency-ordered batching
    tests: a child row's parent may legitimately land in a later
    pagination batch of the same load. use_alter=True since DuckDB can't
    ALTER TABLE ADD this constraint, making it deferred/unenforced like a
    cross-table cycle, rather than a plain inline (and thus DuckDB-enforced)
    self-reference."""

    __tablename__ = "self_referential_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    parent_id: so.Mapped[int | None] = so.mapped_column(
        sa.Integer,
        sa.ForeignKey(f"{SchemaRole.PRIMARY.value}.self_referential_table.id", use_alter=True),
    )


class DemoRecord(Base, FileLoadableTableInterface):
    """Optional text label plus one column of each typed kind, for null-handling tests."""

    __tablename__ = "demo_record"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    demo_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    demo_label: so.Mapped[str | None] = so.mapped_column(sa.String, nullable=True)
    demo_number: so.Mapped[int | None] = so.mapped_column(sa.Integer, nullable=True)
    demo_flag: so.Mapped[bool | None] = so.mapped_column(sa.Boolean, nullable=True)
    demo_date: so.Mapped[date | None] = so.mapped_column(sa.Date, nullable=True)


class DemoRequiredRecord(Base, FileLoadableTableInterface):
    """Same shape as DemoRecord with a required text label."""

    __tablename__ = "demo_required_record"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    demo_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    demo_label: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


class DemoDefaultedRecord(Base, FileLoadableTableInterface):
    """Non-nullable columns with a server default, a scalar default and a callable default."""

    __tablename__ = "demo_defaulted_record"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    demo_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    demo_status: so.Mapped[str] = so.mapped_column(sa.String, nullable=False, server_default="open")
    demo_priority: so.Mapped[int] = so.mapped_column(sa.Integer, nullable=False, default=5)
    demo_stamp: so.Mapped[str] = so.mapped_column(sa.String, nullable=False, default=lambda: "made")
    demo_note: so.Mapped[str | None] = so.mapped_column(sa.String, nullable=True, default="n")


class DemoTypedRecord(Base, FileLoadableTableInterface):
    """One column of each kind a typed (Parquet) input can fill."""

    __tablename__ = "demo_typed_record"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    typed_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    typed_small: so.Mapped[int | None] = so.mapped_column(sa.SmallInteger, nullable=True)
    typed_float: so.Mapped[float | None] = so.mapped_column(sa.Float, nullable=True)
    typed_numeric: so.Mapped[Decimal | None] = so.mapped_column(sa.Numeric(10, 2), nullable=True)
    typed_flag: so.Mapped[bool | None] = so.mapped_column(sa.Boolean, nullable=True)
    typed_date: so.Mapped[date | None] = so.mapped_column(sa.Date, nullable=True)
    typed_stamp: so.Mapped[datetime | None] = so.mapped_column(sa.DateTime, nullable=True)
    typed_stamp_utc: so.Mapped[datetime | None] = so.mapped_column(sa.DateTime(timezone=True), nullable=True)
    typed_label: so.Mapped[str | None] = so.mapped_column(sa.String(10), nullable=True)


class DemoDefaultKinds(Base, FileLoadableTableInterface):
    """One column for each kind of default a column can declare."""

    __tablename__ = "demo_default_kinds"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    kind_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    kind_text: so.Mapped[str] = so.mapped_column(sa.String, nullable=False, server_default="it's text")
    kind_int: so.Mapped[int] = so.mapped_column(sa.Integer, nullable=False, server_default="7")
    kind_bool: so.Mapped[bool] = so.mapped_column(sa.Boolean, nullable=False, server_default=sa.true())
    kind_date: so.Mapped[date] = so.mapped_column(sa.Date, nullable=False, server_default="2000-01-02")
    kind_stamp: so.Mapped[datetime] = so.mapped_column(sa.DateTime, nullable=False, server_default="2000-01-02 10:30:00")
    kind_expression: so.Mapped[str] = so.mapped_column(sa.String, nullable=False, server_default=sa.text("'expr'"))
    kind_now: so.Mapped[datetime | None] = so.mapped_column(sa.DateTime, nullable=True, server_default=sa.func.current_timestamp())
    kind_py_int: so.Mapped[int | None] = so.mapped_column(sa.Integer, nullable=True, default=5)
    kind_py_date: so.Mapped[date | None] = so.mapped_column(sa.Date, nullable=True, default=date(2001, 2, 3))


class DemoBinaryRecord(Base, FileLoadableTableInterface):
    """A column type that the conversion policy has no handler for."""

    __tablename__ = "demo_binary_record"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    demo_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    demo_blob: so.Mapped[str | None] = so.mapped_column(sa.LargeBinary, nullable=True)


class DemoKeyedRecord(Base, FileLoadableTableInterface):
    """A key the database assigns."""

    __tablename__ = "demo_keyed_record"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    keyed_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    keyed_name: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


class DemoBadDefaultRecord(Base, FileLoadableTableInterface):
    """A default that cannot be read as the type of its column."""

    __tablename__ = "demo_bad_default_record"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    bad_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    bad_count: so.Mapped[int | None] = so.mapped_column(sa.Integer, nullable=True, default="abc")
