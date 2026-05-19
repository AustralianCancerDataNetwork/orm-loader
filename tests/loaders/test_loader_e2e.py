from typing import Type, cast

import logging
import numpy as np
import pandas as pd
import pytest
import sqlalchemy as sa
import sqlalchemy.event as sae
import sqlalchemy.orm as so

from orm_loader.loaders.data_classes import _clean_nulls
from orm_loader.loaders.loader_interface import PandasLoader
from orm_loader.tables.loadable_table import CSVLoadableTableInterface
from orm_loader.tables.typing import CSVTableProtocol
from tests.models import Base, CompositeTable, RequiredTable, SimpleTable

# Typed aliases: Pylance cannot verify SQLAlchemy metaclass-generated attrs
# satisfy CSVTableProtocol structurally, so we cast once per class here.
_SimpleTable = cast(Type[CSVTableProtocol], SimpleTable)
_RequiredTable = cast(Type[CSVTableProtocol], RequiredTable)
_CompositeTable = cast(Type[CSVTableProtocol], CompositeTable)


def test_initial_csv_load(session, tmp_path):
    csv_path = tmp_path / "test_table.csv"

    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
            {"id": 3, "name": "gamma"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    loader = PandasLoader()

    inserted = _SimpleTable.load_csv(
        session,
        csv_path,
        dedupe=False,
        loader=loader,
    )
    session.commit()

    assert inserted == 3

    rows = session.execute(sa.select(SimpleTable).order_by(SimpleTable.id)).scalars().all()

    assert [(r.id, r.name) for r in rows] == [
        (1, "alpha"),
        (2, "beta"),
        (3, "gamma"),
    ]


def test_replace_merge_strategy(session, tmp_path):
    csv_path = tmp_path / "test_table.csv"

    # Initial load
    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
            {"id": 3, "name": "gamma"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    loader = PandasLoader()

    _SimpleTable.load_csv(
        session,
        csv_path,
        dedupe=False,
        loader=loader,
    )
    session.commit()

    # Replace rows 2 and 3
    pd.DataFrame(
        [
            {"id": 2, "name": "beta_updated"},
            {"id": 3, "name": "gamma_updated"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    replaced = _SimpleTable.load_csv(
        session,
        csv_path,
        dedupe=False,
        loader=loader,
        merge_strategy="replace",
    )
    session.commit()

    assert replaced == 2

    rows = session.execute(sa.select(SimpleTable).order_by(SimpleTable.id)).scalars().all()

    assert [(r.id, r.name) for r in rows] == [
        (1, "alpha"),
        (2, "beta_updated"),
        (3, "gamma_updated"),
    ]


def test_empty_csv_is_noop(session, tmp_path):
    csv_path = tmp_path / "test_table.csv"
    csv_path.touch()

    loader = PandasLoader()

    inserted = _SimpleTable.load_csv(
        session,
        csv_path,
        dedupe=False,
        loader=loader,
    )
    session.commit()

    assert inserted == 0

    rows = session.execute(sa.select(SimpleTable)).scalars().all()
    assert rows == []


def test_required_column_violation_drops_rows(session, tmp_path):
    csv = tmp_path / "required_table.csv"
    pd.DataFrame(
        [
            {"id": 1, "name": "ok"},
            {"id": 2, "name": None},  # invalid
        ]
    ).to_csv(csv, index=False)

    inserted = _RequiredTable.load_csv(
        session,
        csv,
        loader=PandasLoader(),
        dedupe=False,
    )
    session.commit()

    assert inserted == 1


def test_composite_pk_dedup(session, tmp_path):
    csv = tmp_path / "composite_table.csv"
    pd.DataFrame(
        [
            {"a": 1, "b": 1, "value": "x"},
            {"a": 1, "b": 1, "value": "x"},
            {"a": 1, "b": 2, "value": "y"},
        ]
    ).to_csv(csv, index=False)

    inserted = _CompositeTable.load_csv(
        session,
        csv,
        loader=PandasLoader(),
        dedupe=True,
    )
    session.commit()

    assert inserted == 2


@pytest.mark.parametrize(
    "merge_strategy,expected_rows,expected_inserted",
    [
        (
            "replace",
            [
                (1, "alpha"),
                (2, "beta_updated"),
                (3, "gamma_updated"),
            ],
            2,
        ),
        (
            "upsert",
            [
                (1, "alpha"),
                (2, "beta"),
                (3, "gamma"),
            ],
            2,
        ),
    ],
)
def test_merge_strategies(session, tmp_path, merge_strategy, expected_rows, expected_inserted):
    csv_path = tmp_path / "test_table.csv"

    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
            {"id": 3, "name": "gamma"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    loader = PandasLoader()
    SimpleTable.load_csv(session, csv_path, dedupe=False, loader=loader)
    session.commit()

    pd.DataFrame(
        [
            {"id": 2, "name": "beta_updated"},
            {"id": 3, "name": "gamma_updated"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    inserted = SimpleTable.load_csv(
        session,
        csv_path,
        dedupe=False,
        loader=loader,
        merge_strategy=merge_strategy,
    )
    session.commit()

    assert inserted == expected_inserted

    rows = (
        session.execute(sa.select(SimpleTable).order_by(SimpleTable.id, SimpleTable.name))
        .scalars()
        .all()
    )

    assert [(r.id, r.name) for r in rows] == expected_rows


def test_insert_if_empty_merge_strategy(session, tmp_path):
    csv_path = tmp_path / "test_table.csv"

    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    loader = PandasLoader()
    inserted = _SimpleTable.load_csv(
        session,
        csv_path,
        dedupe=False,
        loader=loader,
        merge_strategy="insert_if_empty",
    )
    session.commit()

    assert inserted == 2

    rows = session.execute(sa.select(SimpleTable).order_by(SimpleTable.id)).scalars().all()

    assert [(r.id, r.name) for r in rows] == [
        (1, "alpha"),
        (2, "beta"),
    ]


def test_insert_if_empty_raises_on_non_empty_target(session, engine, tmp_path):
    csv_path = tmp_path / "test_table.csv"

    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv_path, index=False, sep="\t")

    loader = PandasLoader()
    _SimpleTable.load_csv(session, csv_path, dedupe=False, loader=loader)
    session.commit()

    pd.DataFrame([{"id": 2, "name": "beta"}]).to_csv(csv_path, index=False, sep="\t")

    with pytest.raises(ValueError, match="is not empty; cannot use merge strategy 'insert_if_empty'"):
        _SimpleTable.load_csv(
            session,
            csv_path,
            dedupe=False,
            loader=loader,
            merge_strategy="insert_if_empty",
        )

    inspector = sa.inspect(engine)
    assert not inspector.has_table(SimpleTable.staging_tablename())


@pytest.mark.parametrize("merge_strategy", ["replace", "upsert"])
def test_empty_target_routes_merge_to_insert_if_empty(session, tmp_path, caplog, merge_strategy):
    csv_path = tmp_path / "test_table.csv"

    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    caplog.set_level(logging.INFO, logger="orm_loader.tables.loadable_table")

    inserted = _SimpleTable.load_csv(
        session,
        csv_path,
        dedupe=False,
        loader=PandasLoader(),
        merge_strategy=merge_strategy,
    )
    session.commit()

    assert inserted == 2

    rows = session.execute(sa.select(SimpleTable).order_by(SimpleTable.id)).scalars().all()
    assert [(r.id, r.name) for r in rows] == [
        (1, "alpha"),
        (2, "beta"),
    ]

    messages = [record.getMessage() for record in caplog.records]

    assert any("Checking whether target table is empty for merge optimisation." in message for message in messages)
    assert any(
        f"Target table is empty; routing merge strategy `{merge_strategy}` to insert-if-empty fast path."
        in message
        for message in messages
    )
    assert any("Merge insert-if-empty phase starting." in message for message in messages)
    assert not any("Checking whether target table is empty." in message for message in messages)
    assert not any("Merge replace delete phase starting." in message for message in messages)
    assert not any("Merge upsert phase starting." in message for message in messages)


def test_staging_table_is_created_and_dropped(session, engine, tmp_path):
    csv_path = tmp_path / "test_table.csv"

    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv_path, index=False)

    _SimpleTable.load_csv(
        session,
        csv_path,
        loader=PandasLoader(),
        dedupe=False,
    )
    session.commit()

    inspector = sa.inspect(engine)
    assert not inspector.has_table(SimpleTable.staging_tablename())


def test_composite_pk_replace_merge(session, tmp_path):
    csv = tmp_path / "composite_table.csv"

    pd.DataFrame(
        [
            {"a": 1, "b": 1, "value": "x"},
            {"a": 1, "b": 2, "value": "y"},
        ]
    ).to_csv(csv, index=False)

    CompositeTable.load_csv(session, csv, loader=PandasLoader())
    session.commit()

    pd.DataFrame(
        [
            {"a": 1, "b": 1, "value": "x_updated"},
        ]
    ).to_csv(csv, index=False)

    CompositeTable.load_csv(
        session,
        csv,
        loader=PandasLoader(),
        merge_strategy="replace",
    )
    session.commit()

    rows = (
        session.execute(sa.select(CompositeTable).order_by(CompositeTable.a, CompositeTable.b))
        .scalars()
        .all()
    )

    assert [(r.a, r.b, r.value) for r in rows] == [
        (1, 1, "x_updated"),
        (1, 2, "y"),
    ]


def test_filename_must_match_tablename(session, tmp_path):
    csv = tmp_path / "wrong_name.csv"
    pd.DataFrame([{"id": 1, "name": "x"}]).to_csv(csv, index=False)

    with pytest.raises(ValueError, match="does not match table"):
        SimpleTable.load_csv(
            session,
            csv,
            loader=PandasLoader(),
        )


def test_clean_nulls_basic():
    assert _clean_nulls(None) is None
    assert _clean_nulls(pd.NA) is None
    assert _clean_nulls(float("nan")) is None
    assert _clean_nulls(np.nan) is None


def test_clean_nulls_passthrough():
    assert _clean_nulls("") == ""
    assert _clean_nulls("nan") == "nan"  # string 'nan' must not be converted
    assert _clean_nulls(0) == 0
    assert _clean_nulls("S") == "S"


def test_nullable_column_with_nan_does_not_crash(session, engine, tmp_path):
    class NullableTable(Base, CSVLoadableTableInterface):
        __tablename__ = "nullable_table"

        id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
        flag: so.Mapped[str | None] = so.mapped_column(sa.String, nullable=True)

    Base.metadata.create_all(engine)
    _NullableTable = cast(Type[CSVTableProtocol], NullableTable)

    csv = tmp_path / "nullable_table.csv"
    pd.DataFrame(
        [
            {"id": 1, "flag": "S"},
            {"id": 2, "flag": None},  # becomes NaN in pandas
        ]
    ).to_csv(csv, index=False)

    inserted = _NullableTable.load_csv(
        session,
        csv,
        loader=PandasLoader(),
        dedupe=False,
    )
    session.commit()

    assert inserted == 2

    rows = session.execute(sa.select(NullableTable).order_by(NullableTable.id)).scalars().all()

    assert [(r.id, r.flag) for r in rows] == [
        (1, "S"),
        (2, None),
    ]


def test_embedded_newline_in_field_is_preserved(session, engine, tmp_path):
    class TextTable(Base, CSVLoadableTableInterface):
        __tablename__ = "text_table"

        id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
        name: so.Mapped[str] = so.mapped_column(sa.String)

    Base.metadata.create_all(engine)
    _TextTable = cast(Type[CSVTableProtocol], TextTable)

    csv = tmp_path / "text_table.csv"

    # Properly quoted CSV with embedded newline
    csv.write_text('id\tname\n1\t"hello\nworld"\n')

    _TextTable.load_csv(
        session,
        csv,
        loader=PandasLoader(),
        dedupe=False,
    )
    session.commit()

    rows = session.execute(sa.select(TextTable)).scalars().all()
    assert rows[0].name == "hello\nworld"


def test_embedded_tab_in_field(session, engine, tmp_path):
    class TextTable2(Base, CSVLoadableTableInterface):
        __tablename__ = "tab_table"

        id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
        name: so.Mapped[str] = so.mapped_column(sa.String)

    Base.metadata.create_all(engine)
    _TextTable2 = cast(Type[CSVTableProtocol], TextTable2)

    csv = tmp_path / "tab_table.csv"
    csv.write_text('id\tname\n1\t"foo\tbar"\n')

    _TextTable2.load_csv(
        session,
        csv,
        loader=PandasLoader(),
        dedupe=False,
    )
    session.commit()

    rows = session.execute(sa.select(TextTable2)).scalars().all()
    assert rows[0].name == "foo\tbar"


# --- index_strategy tests ---


def _make_ddl_tracker(engine):
    """Return a list that is populated with DROP/CREATE INDEX statements as they execute."""
    ddl_log: list[str] = []

    def _capture(*args):
        statement: str = args[2]
        upper = statement.strip().upper()
        if upper.startswith("DROP INDEX") or upper.startswith("CREATE INDEX"):
            ddl_log.append(statement.strip())

    sae.listen(engine, "before_cursor_execute", _capture)
    return ddl_log


def test_auto_strategy_keeps_indices_on_sqlite(session, engine, tmp_path):
    """On SQLite, 'auto' resolves to 'keep' — no index DDL should be emitted."""
    ddl_log = _make_ddl_tracker(engine)
    csv_path = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]).to_csv(
        csv_path, index=False, sep="\t"
    )

    _SimpleTable.load_csv(session, csv_path, loader=PandasLoader(), index_strategy="auto")
    session.commit()

    assert not any("DROP INDEX" in s.upper() for s in ddl_log)
    assert not any("CREATE INDEX" in s.upper() for s in ddl_log)
    inspector = sa.inspect(engine)
    inspector.clear_cache()
    assert "ix_test_table_name" in {idx["name"] for idx in inspector.get_indexes("test_table")}


def test_explicit_keep_preserves_indices(session, engine, tmp_path):
    """Explicit 'keep' emits no index DDL regardless of dialect."""
    ddl_log = _make_ddl_tracker(engine)
    csv_path = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv_path, index=False, sep="\t")

    _SimpleTable.load_csv(session, csv_path, loader=PandasLoader(), index_strategy="keep")
    session.commit()

    assert not any("DROP INDEX" in s.upper() for s in ddl_log)
    inspector = sa.inspect(engine)
    inspector.clear_cache()
    assert "ix_test_table_name" in {idx["name"] for idx in inspector.get_indexes("test_table")}


def test_explicit_drop_rebuild_on_sqlite_restores_index(session, engine, tmp_path):
    """Explicit 'drop_rebuild' drops then restores the index even on SQLite."""
    ddl_log = _make_ddl_tracker(engine)
    csv_path = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]).to_csv(
        csv_path, index=False, sep="\t"
    )

    _SimpleTable.load_csv(session, csv_path, loader=PandasLoader(), index_strategy="drop_rebuild")
    session.commit()

    assert any("DROP INDEX" in s.upper() for s in ddl_log)
    assert any("CREATE INDEX" in s.upper() for s in ddl_log)
    inspector = sa.inspect(engine)
    inspector.clear_cache()
    assert "ix_test_table_name" in {idx["name"] for idx in inspector.get_indexes("test_table")}


def test_drop_rebuild_logging_shows_merge_phases(session, tmp_path, caplog):
    csv_path = tmp_path / "test_table.csv"

    pd.DataFrame(
        [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    loader = PandasLoader()
    _SimpleTable.load_csv(session, csv_path, dedupe=False, loader=loader)
    session.commit()

    pd.DataFrame(
        [
            {"id": 2, "name": "beta_updated"},
            {"id": 3, "name": "gamma"},
        ]
    ).to_csv(csv_path, index=False, sep="\t")

    caplog.set_level(logging.INFO, logger="orm_loader.tables.loadable_table")

    _SimpleTable.load_csv(
        session,
        csv_path,
        dedupe=False,
        loader=loader,
        merge_strategy="replace",
        index_strategy="drop_rebuild",
    )
    session.commit()

    messages = [record.getMessage() for record in caplog.records]

    assert any("Dropping 1 active indices." in message for message in messages)
    assert any("Finished dropping 1 active indices in " in message for message in messages)
    assert any("Committing after index drop." in message for message in messages)
    assert any("Commit after index drop completed in " in message for message in messages)
    assert any("Disabling foreign key checks before merge." in message for message in messages)
    assert any("Foreign key checks disabled in " in message for message in messages)
    assert any("Starting merge operation." in message for message in messages)
    assert any("Merge replace delete phase starting." in message for message in messages)
    assert any("Merge replace delete phase completed in " in message for message in messages)
    assert any("Merge insert phase starting." in message for message in messages)
    assert any("Merge insert phase completed in " in message for message in messages)
    assert any("Merge operation SQL completed in " in message for message in messages)
    assert any("Committing merged rows." in message for message in messages)
    assert any("Merge commit completed in " in message for message in messages)
    assert any("Restoring foreign key checks." in message for message in messages)
    assert any("Foreign key checks restored in " in message for message in messages)
    assert any("Verifying/Rebuilding indices." in message for message in messages)
    assert any("Restoring missing index: ix_test_table_name" in message for message in messages)
    assert any("Restored missing index `ix_test_table_name` in " in message for message in messages)
    assert any("Committing restored index `ix_test_table_name`." in message for message in messages)
    assert any(
        "Commit after restoring index `ix_test_table_name` completed in " in message
        for message in messages
    )
    assert any("Index verification/rebuild completed in " in message for message in messages)


def test_invalid_index_strategy_raises(session, tmp_path):
    """An unrecognised strategy value raises ValueError before any DB work."""
    csv_path = tmp_path / "test_table.csv"
    pd.DataFrame([{"id": 1, "name": "alpha"}]).to_csv(csv_path, index=False, sep="\t")

    with pytest.raises(ValueError, match="Unknown index_strategy"):
        _SimpleTable.load_csv(session, csv_path, loader=PandasLoader(), index_strategy="not-valid")


# from hypothesis import given, strategies as st
# from sqlalchemy.orm import declarative_base
# from pathlib import Path

# from orm_loader.loaders.data.converters import perform_cast

# @given(
#     s=st.text(
#         alphabet=st.characters(
#             blacklist_categories=["Cs"],
#             blacklist_characters=["\x00"],
#         )
#     )
# )
# def test_random_strings_roundtrip_respects_casting_contract(s, tmp_path_factory):
#     tmp_path = Path(tmp_path_factory.mktemp("hypothesis_csv"))

#     BaseLocal = declarative_base()

#     class FuzzTable(BaseLocal, CSVLoadableTableInterface):
#         __tablename__ = "fuzz_table"
#         id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
#         txt: so.Mapped[str] = so.mapped_column(sa.String)

#     engine = sa.create_engine("sqlite:///:memory:", future=True)
#     BaseLocal.metadata.create_all(engine)

#     with Session(engine) as session:
#         csv = tmp_path / "fuzz_table.csv"
#         pd.DataFrame([{"id": 1, "txt": s}]).to_csv(csv, index=False, sep="\t")

#         FuzzTable.load_csv(session, csv, loader=PandasLoader(), dedupe=False)
#         session.commit()

#         rows = session.execute(sa.select(FuzzTable)).scalars().all()

#         # What the loader *should* produce according to your spec
#         expected = perform_cast(s, sa.String(), on_error=None)

#         if expected is None:
#             assert rows == []
#         else:
#             # stored value may be str-canonicalised version
#             assert rows[0].txt.encode("utf-8", errors="replace") == s.encode("utf-8", errors="replace")
