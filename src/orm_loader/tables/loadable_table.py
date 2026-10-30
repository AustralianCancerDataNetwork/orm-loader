# pyright: reportPrivateUsage=false
"""Staged, file-based loading for ORM tables, whatever the file format.

A table loads a file into a staging table through a reader, the conversion
policy and the backend's bulk sink, then merges the staging table into the
target table. Readers choose the file formats; backends choose the merge
and index handling.
"""
import logging
from collections.abc import Mapping
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter
from typing import Any, Generator, Type

import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import ResolvedDatabase, physical_schema_of
from sqlalchemy.exc import InvalidRequestError, UnboundExecutionError

from ..backends.base import DatabaseBackend
from ..backends.resolve import resolve_backend
from ..conversion import ConversionPolicy
from ..helpers import IngestError
from ..loaders import FileReader, LoadReport, LoaderContext, open_reader, stage_file
from .orm_table import ORMTableBase
from .typing import FileTableProtocol

logger = logging.getLogger(__name__)


def _format_elapsed(seconds: float) -> str:
    """Return a compact, human-readable duration for phase logging."""
    return f"{seconds:.2f}s"


def _require_bind(session: so.Session, mapper: Any | None = None) -> sa.Engine | sa.Connection:
    """Return a bound connectable or raise a stable runtime error."""
    try:
        return session.get_bind(mapper=mapper)
    except (InvalidRequestError, UnboundExecutionError) as exc:
        raise RuntimeError("Session is not bound to an engine") from exc


class FileLoadableTableInterface(ORMTableBase):
    """
    Mixin for ORM tables that load from files through a staging table.

    A file is read in batches by a reader, converted under a conversion
    policy, written to a staging table by the backend's bulk sink, and
    merged into the target table. It supports:
    - dialect-aware staging table creation
    - delimited text and Parquet files, and other formats through readers
    - configurable merge strategies
    - explicit staging table lifecycle management

    The class is designed for controlled ingestion pipelines and does
    not attempt to provide concurrency guarantees.
    """

    __abstract__ = True

    @classmethod
    @contextmanager
    def manage_indices(
        cls: Type['FileTableProtocol'],
        session: so.Session,
        index_strategy: str = "auto",
        *,
        backend: DatabaseBackend,
    ) -> Generator[None, None, None]:
        """
        Manage non-primary-key indexes around a staged merge.

        ``index_strategy`` may be ``"auto"``, ``"drop_rebuild"``, or
        ``"keep"``. The backend decides what ``"auto"`` means. At the
        moment SQLite keeps indexes by default, while PostgreSQL drops
        and rebuilds them.

        Raises
        ------
        IngestError
            An index could not be rebuilt after a merge that was committed.
        """
        resolved_index_strategy = backend.resolve_index_strategy(index_strategy)
        table_name = cls.__tablename__

        indices = list(cls.__table__.indexes) if resolved_index_strategy == "drop_rebuild" else []

        if indices:
            connection = session.connection(bind_arguments={"mapper": cls})
            inspector = sa.inspect(connection)
            schema = physical_schema_of(connection, schema_tag=cls.__table__.schema)
            existing_in_db = {idx['name'] for idx in inspector.get_indexes(cls.__tablename__, schema=schema)}
            to_drop = [i for i in indices if i.name in existing_in_db]
            
            if to_drop:
                logger.info(f"Table `{table_name}`: Dropping {len(to_drop)} active indices.")
                drop_started = perf_counter()
                for idx in to_drop:
                    session.execute(sa.schema.DropIndex(idx))
                logger.info(
                    f"Table `{table_name}`: Finished dropping {len(to_drop)} active indices "
                    f"in {_format_elapsed(perf_counter() - drop_started)}."
                )
                logger.info(f"Table `{table_name}`: Committing after index drop.")
                commit_started = perf_counter()
                session.commit()
                logger.info(
                    f"Table `{table_name}`: Commit after index drop completed in "
                    f"{_format_elapsed(perf_counter() - commit_started)}."
                )

        merge_failed = False
        try:
            yield
            logger.info(f"Table `{table_name}`: Committing merged rows.")
            commit_started = perf_counter()
            session.commit()
            logger.info(
                f"Table `{table_name}`: Merge commit completed in "
                f"{_format_elapsed(perf_counter() - commit_started)}."
            )
        except Exception as e:
            merge_failed = True
            session.rollback()
            logger.error(f"Table `{table_name}`: Merge operation failed - {e}")
            raise
        finally:
            if indices:
                try:
                    failed_indices = cls._restore_indices(session, indices)
                except Exception as e:
                    if not merge_failed:
                        raise
                    logger.error(f"Table `{table_name}`: Index rebuild after the failed merge also failed - {e}")
                else:
                    if failed_indices and not merge_failed:
                        raise IngestError(
                            f"Table `{table_name}`: the merge was committed but index(es) {failed_indices} could not be rebuilt."
                        )


    @classmethod
    def _restore_indices(
        cls: Type['FileTableProtocol'],
        session: so.Session,
        indices: list[sa.Index],
    ) -> list[str]:
        """
        Recreate the given indexes that are missing from the table.

        Parameters
        ----------
        session
            An active SQLAlchemy session.
        indices
            Indexes the table is expected to have.

        Returns
        -------
        list[str]
            Names of the indexes that could not be recreated.
        """
        table_name = cls.__tablename__
        failed_indices: list[str] = []
        logger.info(f"Table `{table_name}`: Verifying/Rebuilding indices.")
        rebuild_started = perf_counter()
        connection = session.connection(bind_arguments={"mapper": cls})
        inspector = sa.inspect(connection)
        schema = physical_schema_of(connection, schema_tag=cls.__table__.schema)
        existing_idx_names = {idx['name'] for idx in inspector.get_indexes(table_name, schema=schema)}

        for idx in indices:
            if idx.name not in existing_idx_names:
                try:
                    logger.info(f"Table `{table_name}`: Restoring missing index: {idx.name}")
                    create_started = perf_counter()
                    session.execute(sa.schema.CreateIndex(idx))
                    logger.info(
                        f"Table `{table_name}`: Restored missing index `{idx.name}` in "
                        f"{_format_elapsed(perf_counter() - create_started)}."
                    )
                    logger.info(f"Table `{table_name}`: Committing restored index `{idx.name}`.")
                    commit_started = perf_counter()
                    session.commit()
                    logger.info(
                        f"Table `{table_name}`: Commit after restoring index `{idx.name}` "
                        f"completed in {_format_elapsed(perf_counter() - commit_started)}."
                   )
                except Exception as e:
                    session.rollback()
                    failed_indices.append(str(idx.name))
                    logger.error(f"Table `{table_name}`: Failed to restore {idx.name}: {e}")
            else:
                logger.debug(f"Table `{table_name}`: Index {idx.name} already exists on disk. Skipping.")
        logger.info(
            f"Table `{table_name}`: Index verification/rebuild completed in "
            f"{_format_elapsed(perf_counter() - rebuild_started)}."
        )
        return failed_indices


    @classmethod
    def load_file(
        cls: Type[FileTableProtocol],
        session: so.Session,
        path: Path,
        *,
        reader: FileReader | None = None,
        normalise: bool = True,
        dedupe: bool = False,
        chunksize: int | None = None,
        merge_strategy: str = "replace",
        quote_mode: str | None = None,
        index_strategy: str = "auto",
        merge_batch_size: int | None = None,
        staging_schema_tag: str | None = None,
        validate_deferred_fks: bool = True,
        resolved: ResolvedDatabase | None = None,
        physical_schemas: Mapping[str, str | None] | None = None,
        policy: ConversionPolicy | None = None,
        rejects_dir: Path | None = None,
        report: LoadReport | None = None,
    ) -> int:

        """
        Load a file into the target table.

        The file name without its suffix must equal the table name. This
        method orchestrates the full staged ingestion lifecycle:
        - staging table creation
        - file loading
        - merge into the target table
        - staging table cleanup

        Parameters
        ----------
        session
            An active SQLAlchemy session.
        path
            Path to the input file.
        reader
            Reader for the file. ``open_reader`` chooses one from the file
            suffix when omitted, and ``chunksize`` and ``quote_mode`` then
            configure it. Passing either together with a reader is an error, because the
            reader is already configured.
        normalise
            Whether to convert values to the column types under the policy.
            When false, values are written as read.
        dedupe
            Whether to deduplicate incoming rows.
        chunksize
            Rows per batch for the default reader of formats that batch by
            rows.
        merge_strategy
            Merge strategy to apply (e.g. ``replace``, ``upsert``, or
            ``insert``).
        quote_mode
            How double quotes in delimited text are read: ``"csv"`` (field
            quoting), ``"literal"`` (kept as data), ``"by_delimiter"``
            (literal for tab-delimited files, field quoting otherwise).
        index_strategy
            Index handling strategy during merge. Use ``"auto"`` to let
            the backend choose a sensible default.
        staging_schema_tag
            schema_translate_map tag the staging table lives in. ``None``
            means no schema qualification (backend-default behavior).
            Threaded through every internal step of the load lifecycle so
            they all resolve the same backend/schema.
        validate_deferred_fks
            After merging, run an anti-join check for any FK this backend's
            DB can't enforce itself (see DatabaseBackend.validate_deferred_fks()).
            Also run before every intermediate commit a batched merge makes,
            so a violation is caught before that batch commits rather than
            only after the whole load, when an earlier batch's commit can
            no longer be rolled back. Every other key is verified by
            ``validate_foreign_keys``.
        resolved
            Topology this table's engine(s) were resolved from. Required on
            DuckDB whenever table_cls has any foreign key at all, to
            classify each as enforced or omitted via oa-configurator's
            ``unenforced_foreign_keys()``; omitting it raises rather than
            guessing. Ignored by every other backend, and by a DuckDB
            table with no foreign keys.
        physical_schemas
            A schema tag's physical name beyond resolved's own configured
            ones. Forwarded alongside resolved.
        policy
            How values are interpreted and what happens to unreadable ones.
            The default policy preserves nonempty text and treats only empty
            values as missing in text columns.
        rejects_dir
            Directory that rejected rows are written to when the policy sets
            ``on_error="reject_row"``, as ``<file name>.rejects.csv``.
            Defaults to a ``rejects`` directory inside the source file's
            directory.
        report
            Receives the load's row accounting. A new report is used when
            omitted.

        Returns
        -------
        int
            Number of rows loaded into staging before merge.
        """

        logger.debug(f"Table `{cls.__tablename__}`: Loading file {path}")

        if path.stem.lower() != cls.__tablename__:
            raise ValueError(
                f"File name '{path.name}' does not match table '{cls.__tablename__}'"
            )

        _require_bind(session, cls)
        if reader is not None and (chunksize is not None or quote_mode is not None):
            raise ValueError("chunksize and quote_mode configure the default reader; set them on the reader passed as `reader`")
        policy = policy or ConversionPolicy()
        if reader is None:
            reader = open_reader(
                path,
                cls.model_columns(),
                nulls=policy.nulls,
                quote_mode=quote_mode,
                chunksize=chunksize,
                skip_malformed=policy.errors.on_error == "reject_row",
            )

        backend = resolve_backend(
            session,
            staging_schema_tag=staging_schema_tag,
            mapper=cls,
            resolved=resolved,
            physical_schemas=physical_schemas,
        )
        if (
            merge_strategy == "replace"
            and not backend.supports_replace_with_kept_indexes
            and backend.resolve_index_strategy(index_strategy) == "keep"
        ):
            raise NotImplementedError(
                f"Backend '{backend.name}' does not support merge_strategy='replace' with index_strategy='keep'"
            )

        loader_context = LoaderContext(
            tableclass=cls,
            session=session,
            path=path,
            backend=backend,
            normalise=normalise,
            dedupe=dedupe,
            policy=policy,
            rejects_dir=rejects_dir,
            report=report or LoadReport(),
        )

        try:
            # SQLite cannot switch foreign key checks inside a transaction, so relax before staging writes.
            with backend.relaxed_foreign_keys(session, mapper=cls, no_autoflush=False):
                logger.info(f"Table `{cls.__tablename__}`: Loading data into staging table")
                staging = backend.create_staging_table(cls, session)
                total = stage_file(loader_context, reader, staging)

                logger.info(f"Table `{cls.__tablename__}`: Merging staging data into target table")
                with cls.manage_indices(session, index_strategy, backend=backend):
                    cls.merge_from_staging(
                        session,
                        merge_strategy,
                        backend=backend,
                        staging=staging,
                        merge_batch_size=merge_batch_size,
                        validate_deferred_fks=validate_deferred_fks,
                    )
                    if validate_deferred_fks:
                        logger.info(f"Table `{cls.__tablename__}`: Validating deferred foreign keys.")
                        backend.validate_deferred_fks(cls, session)
        finally:
            backend.drop_staging_table(cls, session)

        logger.info(f"Table `{cls.__tablename__}`: Successfully finished ingestion. Total rows: {total}")
        return total
        

    @classmethod
    def _target_has_rows(
        cls: Type[FileTableProtocol],
        session: so.Session,
    ) -> bool:
        """
        Return whether the target table currently contains any rows.
        """
        row = session.execute(
            sa.select(sa.literal(1)).select_from(cls.__table__).limit(1),
            bind_arguments={"mapper": cls},
        ).first()
        return row is not None


    @classmethod
    def validate_foreign_keys(
        cls: Type[FileTableProtocol],
        session: so.Session,
        *,
        staging_schema_tag: str | None = None,
        resolved: ResolvedDatabase | None = None,
        physical_schemas: Mapping[str, str | None] | None = None,
    ) -> None:
        """
        Raise if any row references a row that does not exist.

        Loads relax foreign keys during the merge on PostgreSQL and SQLite,
        so a row loaded before the row it references is accepted. Call this
        once every table is loaded to verify referential integrity,
        whatever the load order. DuckDB enforces most of its keys itself, so
        only the keys it cannot enforce are checked.

        Parameters
        ----------
        session
            An active SQLAlchemy session.
        staging_schema_tag
            schema_translate_map tag used to resolve the backend, as in
            ``load_file``.
        resolved
            Topology this table's engine(s) were resolved from. Required on
            DuckDB for a table with foreign keys.
        physical_schemas
            A schema tag's physical name beyond resolved's own configured
            ones.

        Raises
        ------
        ForeignKeyViolationError
            A non-null reference has no matching row in the referred table.
        """
        _require_bind(session, cls)
        backend = resolve_backend(
            session,
            staging_schema_tag=staging_schema_tag,
            mapper=cls,
            resolved=resolved,
            physical_schemas=physical_schemas,
        )
        backend.validate_foreign_keys(cls, session)

    @classmethod
    def merge_from_staging(
        cls: Type[FileTableProtocol],
        session: so.Session,
        merge_strategy: str = "replace",
        *,
        backend: DatabaseBackend,
        staging: sa.Table,
        merge_batch_size: int | None = None,
        validate_deferred_fks: bool = True,
    ):
        """
        Merge data from the staging table into the target table.

        Parameters
        ----------
        session
            An active SQLAlchemy session.
        merge_strategy
            Merge strategy to apply (for example ``replace``,
            ``upsert``, or ``insert``).
        backend
            Backend of the session's database.
        staging
            Staging table the rows are merged from.
        merge_batch_size
            Rows per merge batch, or ``None`` for one statement.
        validate_deferred_fks
            Forwarded to the backend's merge methods, which validate
            deferred FKs before every intermediate commit a batched merge
            makes. See ``load_file()``.
        """
        target = cls.__tablename__
        pk_cols = cls.pk_names()

        if merge_strategy == "replace":
            logger.info(f"Table `{target}`: Merge replace delete phase starting.")
            delete_started = perf_counter()
            backend.merge_replace(
                cls, session, pk_cols, staging, merge_batch_size=merge_batch_size, validate_deferred_fks=validate_deferred_fks
            )
            logger.info(
                f"Table `{target}`: Merge replace delete phase completed in "
                f"{_format_elapsed(perf_counter() - delete_started)}."
            )
            logger.info(f"Table `{target}`: Merge insert phase starting.")
            insert_started = perf_counter()
            backend.merge_insert(
                cls, session, staging, merge_batch_size=merge_batch_size, validate_deferred_fks=validate_deferred_fks
            )
            logger.info(
                f"Table `{target}`: Merge insert phase completed in "
                f"{_format_elapsed(perf_counter() - insert_started)}."
            )
        elif merge_strategy == "upsert":
            if not cls._target_has_rows(session=session):
                logger.info(
                    f"Table `{target}`: The target table is empty, so merge strategy `insert` "
                    "with `dedupe=True` loads the same rows faster."
                )
            logger.info(f"Table `{target}`: Merge upsert phase starting.")
            upsert_started = perf_counter()
            backend.merge_upsert(
                cls, session, pk_cols, staging, merge_batch_size=merge_batch_size, validate_deferred_fks=validate_deferred_fks
            )
            logger.info(
                f"Table `{target}`: Merge upsert phase completed in "
                f"{_format_elapsed(perf_counter() - upsert_started)}."
            )
        elif merge_strategy == "insert":
            logger.info(f"Table `{target}`: Merge insert phase starting.")
            insert_started = perf_counter()
            backend.merge_insert(
                cls, session, staging, merge_batch_size=merge_batch_size, validate_deferred_fks=validate_deferred_fks
            )
            logger.info(
                f"Table `{target}`: Merge insert phase completed in "
                f"{_format_elapsed(perf_counter() - insert_started)}."
            )
        else:
            raise ValueError(f"Unknown merge strategy '{merge_strategy}'")
