"""Regression coverage for PR #46's ingestion review findings."""

import csv
import inspect
from datetime import date
from typing import cast

import pyarrow as pa
import pytest
import sqlalchemy as sa
import sqlalchemy.orm as so

from orm_loader.backends import STAGING_SCHEMA
from orm_loader.conversion import ConversionPolicy, ErrorPolicy, NullPolicy
from orm_loader.helpers import IngestError, PolicyError
from orm_loader.loaders import DelimitedTextReader, LoadReport, infer_delim
from orm_loader.loaders.processor import BatchProcessor
from orm_loader.tables import FileLoadableTableInterface
from orm_loader.tables.typing import FileTableProtocol, ORMTableProtocol


class ReviewBase(so.DeclarativeBase):
    pass


class ReviewRecord(ReviewBase, FileLoadableTableInterface):
    __tablename__ = "review_record"
    record_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True, autoincrement=False)
    score: so.Mapped[int] = so.mapped_column(sa.Integer, nullable=False)
    note: so.Mapped[str | None] = so.mapped_column(sa.String)
    event_date: so.Mapped[date | None] = so.mapped_column(sa.Date)
    twice: so.Mapped[int | None] = so.mapped_column(sa.Integer, sa.Computed("score * 2"))


class RequiredComputedRecord(ReviewBase, FileLoadableTableInterface):
    __tablename__ = "review_required_computed"
    record_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    score: so.Mapped[int] = so.mapped_column(sa.Integer, nullable=False)
    twice: so.Mapped[int] = so.mapped_column(sa.Integer, sa.Computed("score * 2"), nullable=False)


class KeyedRecord(ReviewBase, FileLoadableTableInterface):
    __tablename__ = "review_keyed_record"
    record_id: so.Mapped[int] = so.mapped_column(
        "id_col", sa.Integer, key="id_key", primary_key=True, autoincrement=False,
    )
    value: so.Mapped[int] = so.mapped_column("val_col", sa.Integer, key="val_key")


class CompositeKeyedRecord(ReviewBase, FileLoadableTableInterface):
    __tablename__ = "review_composite_keyed_record"
    record_id: so.Mapped[int] = so.mapped_column(
        "id_col", sa.Integer, key="id_key", primary_key=True, autoincrement=False,
    )
    group_id: so.Mapped[int] = so.mapped_column(
        "group_col", sa.Integer, key="group_key", primary_key=True, autoincrement=False,
    )
    value: so.Mapped[int] = so.mapped_column("val_col", sa.Integer, key="val_key")


class ExpressionDefaultRecord(ReviewBase, FileLoadableTableInterface):
    __tablename__ = "review_expression_default"
    record_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True, autoincrement=False)
    token: so.Mapped[float] = so.mapped_column(sa.Float, server_default=sa.func.random())


class DefaultKeyRecord(ReviewBase, FileLoadableTableInterface):
    __tablename__ = "review_default_key"
    record_id: so.Mapped[int] = so.mapped_column(
        sa.Integer, primary_key=True, nullable=True, default=99, autoincrement=False,
    )
    note: so.Mapped[str | None] = so.mapped_column(sa.String)


class InvalidNumericRecord(ReviewBase, FileLoadableTableInterface):
    __tablename__ = "review_invalid_numeric"
    record_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    amount: so.Mapped[float] = so.mapped_column(sa.Numeric(2, 5))


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


@pytest.fixture
def review_session(dialect_session):
    _create(dialect_session, ReviewRecord)
    return dialect_session


def test_malformed_row_ratio_counts_the_current_parsed_block(review_session, tmp_path):
    policy = ConversionPolicy(errors=ErrorPolicy(on_error="reject_row", max_rejects=0.01))
    report = LoadReport()
    loaded = _load(
        review_session, tmp_path, ReviewRecord,
        "record_id,score\n1,2,3\n" + "".join(f"{i},{i}\n" for i in range(2, 201)),
        policy=policy, report=report, rejects_dir=tmp_path,
    )
    assert loaded == 199
    assert (report.rows_read, report.rows_loaded, report.rows_rejected) == (200, 199, 1)
    assert report.accounted


def test_unreadable_and_missing_values_keep_separate_rejection_reasons(review_session, tmp_path):
    policy = ConversionPolicy(errors=ErrorPolicy(on_error="reject_row"))
    _load(
        review_session, tmp_path, ReviewRecord,
        "record_id,score\n1,bad\n2,\n3,3\n", policy=policy, rejects_dir=tmp_path,
    )
    with (tmp_path / "review_record.rejects.csv").open(newline="") as handle:
        rejected = list(csv.DictReader(handle))
    assert [(row["row"], row["column"], row["reason"]) for row in rejected] == [
        ("1", "score", "cannot be read as Integer"),
        ("2", "score", "required value is missing"),
    ]
    assert review_session.scalars(sa.select(ReviewRecord.record_id)).all() == [3]


def test_empty_text_can_be_preserved(review_session, tmp_path):
    policy = ConversionPolicy(nulls=NullPolicy(empty_is_null=False))
    _load(
        review_session, tmp_path, ReviewRecord,
        "record_id,score,note\n1,2,\n", policy=policy,
    )
    assert review_session.scalar(sa.select(ReviewRecord.note)) == ""


@pytest.mark.parametrize("supplied_reader", [False, True])
def test_computed_columns_are_never_loaded(review_session, tmp_path, supplied_reader):
    text = "record_id,score,twice\n1,2,wrong\n"
    kwargs = {}
    if supplied_reader:
        path = tmp_path / "review_record.csv"
        path.write_text(text)
        kwargs["reader"] = DelimitedTextReader(path, ReviewRecord.model_columns())
    _load(review_session, tmp_path, ReviewRecord, text, **kwargs)
    assert review_session.scalar(sa.select(ReviewRecord.twice)) == 4


def test_supplied_reader_must_share_the_conversion_null_policy(review_session, tmp_path):
    path = tmp_path / "review_record.csv"
    path.write_text("record_id,score\n1,1\n")
    reader = DelimitedTextReader(path, ReviewRecord.model_columns())
    policy = ConversionPolicy(nulls=NullPolicy(empty_is_null=False))
    with pytest.raises(ValueError, match="reader's null policy differs"):
        _load(review_session, tmp_path, ReviewRecord, path.read_text(), reader=reader, policy=policy)


def test_tab_markers_ignore_padding_without_quote_semantics(review_session, tmp_path):
    path = tmp_path / "review_record.tsv"
    path.write_text(
        "record_id\tscore\tnote\tevent_date\n"
        "1\t2\t MISSING \t MISSING \n2\t3\tMISSING\tMISSING\n"
    )
    policy = ConversionPolicy(nulls=NullPolicy(
        text_markers=frozenset({"MISSING"}), marker_quoting="unquoted_only",
    ))
    reader = DelimitedTextReader(path, ReviewRecord.model_columns(), nulls=policy.nulls)
    _load(review_session, tmp_path, ReviewRecord, "unused", reader=reader, policy=policy)
    rows = review_session.execute(sa.select(ReviewRecord.note, ReviewRecord.event_date)).all()
    assert [tuple(row) for row in rows] == [(None, None), (None, None)]


def test_legacy_text_markers_cover_all_case_variants(review_session, tmp_path):
    markers = sorted(NullPolicy.LEGACY_TEXT_MARKERS)
    assert {"NULL", "nUlL", "None", "nan", "N/a", "#N/A"} <= set(markers)
    text = "record_id,score,note\n" + "".join(
        f"{i},1,{marker}\n" for i, marker in enumerate([*markers, "keep"], 1)
    )
    policy = ConversionPolicy(nulls=NullPolicy(text_markers=NullPolicy.LEGACY_TEXT_MARKERS))
    _load(review_session, tmp_path, ReviewRecord, text, policy=policy)
    notes = review_session.scalars(sa.select(ReviewRecord.note).order_by(ReviewRecord.record_id)).all()
    assert notes == [None] * len(markers) + ["keep"]


@pytest.mark.parametrize("normalise", [False, True])
def test_defaulted_nullable_primary_key_must_be_in_the_file(dialect_session, tmp_path, normalise):
    _create(dialect_session, DefaultKeyRecord)
    with pytest.raises(IngestError, match="record_id"):
        _load(dialect_session, tmp_path, DefaultKeyRecord, "note\na\n", normalise=normalise)


def test_null_primary_key_is_rejected_instead_of_taking_its_default(dialect_session, tmp_path):
    _create(dialect_session, DefaultKeyRecord)
    policy = ConversionPolicy(errors=ErrorPolicy(on_error="reject_row"))
    _load(
        dialect_session, tmp_path, DefaultKeyRecord,
        "record_id,note\n,a\n1,b\n", policy=policy, rejects_dir=tmp_path,
    )
    assert dialect_session.scalars(sa.select(DefaultKeyRecord.record_id)).all() == [1]


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
@pytest.mark.parametrize("normalise", [False, True])
def test_sql_expression_defaults_are_evaluated_for_each_merged_row(
    dialect_session, tmp_path, strategy, batch_size, normalise,
):
    _create(dialect_session, ExpressionDefaultRecord)
    table = ExpressionDefaultRecord
    if strategy != "insert":
        _load(dialect_session, tmp_path, table, "record_id,token\n1,42\n", merge_strategy="insert")
    text = "record_id,token\n" + "".join(f"{i},\n" for i in range(1, 11)) + "11,123\n"
    _load(
        dialect_session, tmp_path, table, text, merge_strategy=strategy,
        merge_batch_size=batch_size, normalise=normalise,
    )
    tokens = dialect_session.scalars(sa.select(table.token).order_by(table.record_id)).all()
    assert len(tokens) == 11
    assert all(value is not None for value in tokens)
    assert len(set(tokens[:10])) == 10
    assert tokens[-1] == 123
    if strategy == "upsert":
        assert tokens[0] == 42


def test_unsupported_numeric_declaration_names_the_table_and_column(tmp_path):
    with pytest.raises(PolicyError, match=r"review_invalid_numeric.amount.*\(2, 5\)"):
        BatchProcessor(
            cast(type[FileTableProtocol], InvalidNumericRecord), ["record_id", "amount"],
            ConversionPolicy(), LoadReport(), tmp_path / "rejects.csv",
        )


def test_infer_delim_remains_importable_from_loaders(tmp_path):
    path = tmp_path / "data.tsv"
    path.write_text("a\tb\n")
    assert infer_delim(path) == "\t"


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


def test_defaulted_nullable_primary_key_is_always_required():
    assert DefaultKeyRecord.required_columns() == {"record_id"}
    assert "record_id" not in DefaultKeyRecord.column_defaults()


def test_column_defaults_protocol_has_a_callable_classmethod_function():
    descriptor = vars(ORMTableProtocol)["column_defaults"]
    assert isinstance(descriptor, classmethod)
    assert inspect.isfunction(descriptor.__func__)
