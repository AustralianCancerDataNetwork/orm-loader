
from enum import Enum

import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import Role as SchemaRole
from sqlalchemy.orm import declarative_base

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
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}
    id = sa.Column(sa.Integer, primary_key=True)
    value = sa.Column(sa.String, nullable=False)


class SimpleTable(Base, CSVLoadableTableInterface):
    __tablename__ = "test_table"
    __table_args__ = (
        sa.Index("ix_test_table_name", "name"),
        {"schema": SchemaRole.PRIMARY.value},
    )

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


class RequiredTable(Base, CSVLoadableTableInterface):
    __tablename__ = "required_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


class CompositeTable(Base, CSVLoadableTableInterface):
    __tablename__ = "composite_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    a: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    b: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    value: so.Mapped[str] = so.mapped_column(sa.String)


class CycleATable(Base, CSVLoadableTableInterface):
    """Mutually-referencing pair with CycleBTable, for deferred-FK tests:
    DuckDB can't execute the ALTER TABLE ADD CONSTRAINT this cycle forces."""

    __tablename__ = "cycle_a_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    b_id: so.Mapped[int | None] = so.mapped_column(
        sa.Integer, sa.ForeignKey(f"{SchemaRole.PRIMARY.value}.cycle_b_table.id")
    )


class CycleBTable(Base, CSVLoadableTableInterface):
    __tablename__ = "cycle_b_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    a_id: so.Mapped[int | None] = so.mapped_column(
        sa.Integer, sa.ForeignKey(f"{SchemaRole.PRIMARY.value}.cycle_a_table.id")
    )


class EnumTable(Base, CSVLoadableTableInterface):
    """A genuine sa.Enum column, unmodified -- register_column_cast_rule's
    enum_type matches a raw value (e.g. "first author") by .value but stores
    the matching member's .name, which is exactly sa.Enum's own default
    column-storage convention -- no values_callable or other customisation
    needed for this, the common case.
    """

    __tablename__ = "enum_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    role: so.Mapped[Role | None] = so.mapped_column(sa.Enum(Role), nullable=True)


class ComputedColumnTable(Base, CSVLoadableTableInterface):
    """A real, registered table with a computed column, for merge-method
    tests that need get_staging_table() to work. Unlike a bare
    __tablename__/__table__ pair, this actually implements
    CSVLoadableTableInterface."""

    __tablename__ = "computed_column_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String)
    # nullable: DuckDB doesn't support any constraint (including NOT NULL) on a generated column.
    slug: so.Mapped[str | None] = so.mapped_column(sa.String, sa.Computed("lower(name)"))


class ImpliedEnumTable(Base, CSVLoadableTableInterface):
    """A plain String column with no type-level enum signal at all -- the
    OMOP CDM concept.standard_concept/invalid_reason shape register_column_cast_rule
    exists to cover, since sa.Enum-keyed dispatch can never reach a column like this.
    """

    __tablename__ = "implied_enum_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    flag: so.Mapped[str | None] = so.mapped_column(sa.String(1), nullable=True)


class ReferencedParentTable(Base, CSVLoadableTableInterface):
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


class UnreferencedGrandparentTable(Base, CSVLoadableTableInterface):
    __tablename__ = "unreferenced_grandparent_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String)


class ReferencingChildTable(Base, CSVLoadableTableInterface):
    __tablename__ = "referencing_child_table"
    __table_args__ = {"schema": SchemaRole.PRIMARY.value}

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    parent_id: so.Mapped[int] = so.mapped_column(
        sa.Integer, sa.ForeignKey(f"{SchemaRole.PRIMARY.value}.referenced_parent_table.id")
    )


class VocabSchemaTable(Base, CSVLoadableTableInterface):
    """A VOCAB-tagged table, so tests can prove the staging/index schema
    derivation actually resolves a non-primary schema tag correctly."""

    __tablename__ = "test_vocab_role_table"
    __table_args__ = (
        sa.Index("ix_test_vocab_role_table_name", "name"),
        {"schema": SchemaRole.VOCAB.value},
    )

    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    name: so.Mapped[str] = so.mapped_column(sa.String, nullable=False)


class VocabReferencingTable(Base, CSVLoadableTableInterface):
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


class SelfReferentialTable(Base, CSVLoadableTableInterface):
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
