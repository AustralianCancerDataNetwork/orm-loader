"""Columns a file load treats specially: computed, renamed, defaulted and primary-key columns."""

from typing import cast

import pyarrow as pa
import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so

from orm_loader.backends import STAGING_SCHEMA
from orm_loader.conversion import ConversionPolicy, ErrorPolicy
from orm_loader.helpers import IngestError, PolicyError
from orm_loader.loaders import DelimitedTextReader, LoadReport
from orm_loader.loaders.processor import BatchProcessor
from orm_loader.tables import FileLoadableTableInterface
from orm_loader.tables.typing import FileTableProtocol


class SpecialBase(so.DeclarativeBase):
    pass


class ComputedRecord(SpecialBase, FileLoadableTableInterface):
    __tablename__ = "special_computed"
    record_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True, autoincrement=False)
    score: so.Mapped[int] = so.mapped_column(sa.Integer, nullable=False)
    twice: so.Mapped[int | None] = so.mapped_column(sa.Integer, sa.Computed("score * 2"))


class RequiredComputedRecord(SpecialBase, FileLoadableTableInterface):
    __tablename__ = "special_required_computed"
    record_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    score: so.Mapped[int] = so.mapped_column(sa.Integer, nullable=False)
    twice: so.Mapped[int] = so.mapped_column(sa.Integer, sa.Computed("score * 2"), nullable=False)


class KeyedRecord(SpecialBase, FileLoadableTableInterface):
    __tablename__ = "special_keyed"
    record_id: so.Mapped[int] = so.mapped_column(
        "id_col", sa.Integer, key="id_key", primary_key=True, autoincrement=False,
    )
    value: so.Mapped[int] = so.mapped_column("val_col", sa.Integer, key="val_key")


class CompositeKeyedRecord(SpecialBase, FileLoadableTableInterface):
    __tablename__ = "special_composite_keyed"
    record_id: so.Mapped[int] = so.mapped_column(
        "id_col", sa.Integer, key="id_key", primary_key=True, autoincrement=False,
    )
    group_id: so.Mapped[int] = so.mapped_column(
        "group_col", sa.Integer, key="group_key", primary_key=True, autoincrement=False,
    )
    value: so.Mapped[int] = so.mapped_column("val_col", sa.Integer, key="val_key")


class ExpressionDefaultRecord(SpecialBase, FileLoadableTableInterface):
    __tablename__ = "special_expression_default"
    record_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True, autoincrement=False)
    token: so.Mapped[float] = so.mapped_column(sa.Float, server_default=sa.func.random())


class DefaultKeyRecord(SpecialBase, FileLoadableTableInterface):
    __tablename__ = "special_default_key"
    record_id: so.Mapped[int] = so.mapped_column(
        sa.Integer, primary_key=True, nullable=True, default=99, autoincrement=False,
    )
    note: so.Mapped[str | None] = so.mapped_column(sa.String)


class InvalidNumericRecord(SpecialBase, FileLoadableTableInterface):
    __tablename__ = "special_invalid_numeric"
    record_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    amount: so.Mapped[float] = so.mapped_column(sa.Numeric(2, 5))


class UnboundedNumericRecord(SpecialBase, FileLoadableTableInterface):
    __tablename__ = "special_unbounded_numeric"
    record_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    amount: so.Mapped[float] = so.mapped_column(sa.Numeric())


class WideNumericRecord(SpecialBase, FileLoadableTableInterface):
    __tablename__ = "special_wide_numeric"
    record_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    amount: so.Mapped[float] = so.mapped_column(sa.Numeric(50, 2))


def _create(session, model):
    model.__table__.create(session.connection())
    session.commit()


def _load(session, tmp_path, model, text, **kwargs):
    path = tmp_path / f"{model.__tablename__}.csv"
    path.write_text(text, encoding="utf-8")
    tag = None if session.get_bind().dialect.name == "sqlite" else STAGING_SCHEMA
    table = cast(type[FileTableProtocol], model)
    loaded = table.load_file(session, path, staging_schema_tag=tag, **kwargs)
    session.commit()
    return loaded


@pytest.mark.parametrize("supplied_reader", [False, True])
def test_computed_columns_are_never_loaded(dialect_session, tmp_path, supplied_reader):
    _create(dialect_session, ComputedRecord)
    text = "record_id,score,twice\n1,2,wrong\n"
    kwargs = {}
    if supplied_reader:
        path = tmp_path / "special_computed.csv"
        path.write_text(text)
        kwargs["reader"] = DelimitedTextReader(path, ComputedRecord.model_columns())
    _load(dialect_session, tmp_path, ComputedRecord, text, **kwargs)
    assert dialect_session.scalar(sa.select(ComputedRecord.twice)) == 4


@pytest.mark.parametrize("normalise", [False, True])
def test_nonnullable_computed_column_can_be_absent_from_the_file(tmp_path, normalise):
    table = cast(type[FileTableProtocol], RequiredComputedRecord)
    assert table.required_columns() == {"record_id", "score"}
    processor = BatchProcessor(
        table, ["record_id", "score"], ConversionPolicy(), LoadReport(),
        tmp_path / "rejects.csv", normalise=normalise,
    )
    output = processor.process(pa.record_batch({"record_id": ["1"], "score": ["2"]}))
    assert output.column_names == ["record_id", "score"]
    assert output.num_rows == 1


@pytest.mark.parametrize("model", [KeyedRecord, CompositeKeyedRecord])
@pytest.mark.parametrize("strategy", ["insert", "upsert", "replace"])
@pytest.mark.parametrize("batch_size", [None, 1])
def test_merge_uses_database_names_when_column_keys_differ(
    dialect_session, tmp_path, model, strategy, batch_size,
):
    _create(dialect_session, model)
    composite = model is CompositeKeyedRecord
    header = "id_col,group_col,val_col\n" if composite else "id_col,val_col\n"
    def row(key, value):
        return f"{key},1,{value}\n" if composite else f"{key},{value}\n"

    _load(dialect_session, tmp_path, model, header + row(1, 5), merge_strategy="insert")
    data = row(2, 20) if strategy == "insert" else row(1, 10) + row(2, 20)
    _load(
        dialect_session, tmp_path, model, header + data,
        merge_strategy=strategy, merge_batch_size=batch_size,
    )
    rows = dialect_session.execute(sa.select(model.record_id, model.value).order_by(model.record_id))
    assert [tuple(row) for row in rows] == [(1, 10 if strategy == "replace" else 5), (2, 20)]


@pytest.mark.parametrize("strategy", ["insert", "upsert", "replace"])
@pytest.mark.parametrize("batch_size", [None, 2])
def test_sql_expression_defaults_are_evaluated_for_each_merged_row(dialect_session, tmp_path, strategy, batch_size):
    _create(dialect_session, ExpressionDefaultRecord)
    table = ExpressionDefaultRecord
    if strategy != "insert":
        _load(dialect_session, tmp_path, table, "record_id,token\n1,42\n", merge_strategy="insert")
    text = "record_id,token\n" + "".join(f"{i},\n" for i in range(1, 11)) + "11,123\n"
    _load(dialect_session, tmp_path, table, text, merge_strategy=strategy, merge_batch_size=batch_size)
    tokens = dialect_session.scalars(sa.select(table.token).order_by(table.record_id)).all()
    assert len(tokens) == 11
    assert all(value is not None for value in tokens)
    assert len(set(tokens[:10])) == 10
    assert tokens[-1] == 123
    if strategy == "upsert":
        assert tokens[0] == 42


@pytest.mark.parametrize("normalise", [False, True])
def test_a_sql_expression_default_is_evaluated_for_each_row_of_a_missing_column(dialect_session, tmp_path, normalise):
    _create(dialect_session, ExpressionDefaultRecord)
    text = "record_id\n" + "".join(f"{i}\n" for i in range(1, 11))
    _load(dialect_session, tmp_path, ExpressionDefaultRecord, text, normalise=normalise)
    tokens = dialect_session.scalars(sa.select(ExpressionDefaultRecord.token)).all()
    assert len(set(tokens)) == 10


@pytest.mark.parametrize("normalise", [False, True])
def test_a_primary_key_with_a_default_must_still_be_in_the_file(dialect_session, tmp_path, normalise):
    _create(dialect_session, DefaultKeyRecord)
    with pytest.raises(IngestError, match="record_id"):
        _load(dialect_session, tmp_path, DefaultKeyRecord, "note\na\n", normalise=normalise)


def test_a_blank_primary_key_is_rejected_instead_of_taking_its_default(dialect_session, tmp_path):
    _create(dialect_session, DefaultKeyRecord)
    policy = ConversionPolicy(errors=ErrorPolicy(on_error="reject_row"))
    _load(
        dialect_session, tmp_path, DefaultKeyRecord,
        "record_id,note\n,a\n1,b\n", policy=policy, rejects_dir=tmp_path,
    )
    assert dialect_session.scalars(sa.select(DefaultKeyRecord.record_id)).all() == [1]


def test_a_primary_key_is_required_and_takes_no_default():
    assert DefaultKeyRecord.required_columns() == {"record_id"}
    assert "record_id" not in DefaultKeyRecord.column_defaults()


@pytest.mark.parametrize("model, declared", [
    (InvalidNumericRecord, r"\(2, 5\)"), (UnboundedNumericRecord, r"\(None, None\)"), (WideNumericRecord, r"\(50, 2\)"),
])
def test_unsupported_numeric_declaration_names_the_table_and_column(tmp_path, model, declared):
    with pytest.raises(PolicyError, match=rf"{model.__tablename__}.amount.*{declared}"):
        BatchProcessor(
            cast(type[FileTableProtocol], model), ["record_id", "amount"],
            ConversionPolicy(), LoadReport(), tmp_path / "rejects.csv",
        )
