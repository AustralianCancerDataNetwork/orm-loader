# pyright: reportPrivateUsage=false
"""Staged, file-based loading for ORM tables, whatever the file format.

A table loads a file into a staging table through a reader, the conversion
policy and the backend's bulk sink, then merges the staging table into the
target table. Readers choose the file formats; backends choose the merge
and index handling.
"""
import logging
from collections.abc import Collection, Mapping
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter
from typing import Any, Generator, Type

import sqlalchemy as sa
import sqlalchemy.orm as so
from oa_configurator import ResolvedDatabase, physical_schema_of
from sqlalchemy.exc import InvalidRequestError, UnboundExecutionError

from ..backends.resolve import resolve_backend
from ..conversion import ConversionPolicy
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
    def create_staging_table(
        cls: Type[FileTableProtocol],
        session: so.Session,
        *,
        staging_schema_tag: str | None = None,
        without: Collection[str] = (),
    ):
        """
        Create a fresh staging table for ingestion.

        Any existing staging table with the same name is dropped first.
        The staging table has the target's columns with their types and
        nullability.

        Parameters
        ----------
        session
            An active SQLAlchemy session bound to an engine.
        staging_schema_tag
            schema_translate_map tag the staging table should be created
            under. ``None`` means no schema qualification (backend-default
            behavior).
        without
            Names of target columns the staging table leaves out. The merge
            then leaves them out of its insert.

        Raises
        ------
        RuntimeError
            If the session is not bound to an engine.
        NotImplementedError
            If the database dialect is unsupported.
        """
        _require_bind(session, cls)
        backend = resolve_backend(session, staging_schema_tag=staging_schema_tag, mapper=cls)
        backend.create_staging_table(cls, session, without=without)

    @classmethod
    @contextmanager
    def manage_indices(
        cls: Type['FileTableProtocol'],
        session: so.Session,
        index_strategy: str = "auto",
        *,
        staging_schema_tag: str | None = None,
    ) -> Generator[None, None, None]:
        """
        Manage non-primary-key indexes around a staged merge.

        ``index_strategy`` may be ``"auto"``, ``"drop_rebuild"``, or
        ``"keep"``. The backend decides what ``"auto"`` means. At the
        moment SQLite keeps indexes by default, while PostgreSQL drops
        and rebuilds them.
        """
        backend = resolve_backend(session, staging_schema_tag=staging_schema_tag, mapper=cls)
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

        fk_restore_started: float | None = None
        try:
            logger.info(f"Table `{table_name}`: Disabling foreign key checks before merge.")
            fk_disable_started = perf_counter()
            with backend.merge_context(cls, session):
                logger.info(
                    f"Table `{table_name}`: Foreign key checks disabled in "
                    f"{_format_elapsed(perf_counter() - fk_disable_started)}."
                )
                try:
                    yield
                    logger.info(f"Table `{table_name}`: Committing merged rows.")
                    commit_started = perf_counter()
                    session.commit()
                    logger.info(
                        f"Table `{table_name}`: Merge commit completed in "
                        f"{_format_elapsed(perf_counter() - commit_started)}."
                    )
                finally:
                    logger.info(f"Table `{table_name}`: Restoring foreign key checks.")
                    fk_restore_started = perf_counter()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Table `{table_name}`: Merge operation failed - {e}")
            raise
        finally:
            if fk_restore_started is not None:
                logger.info(
                    f"Table `{table_name}`: Foreign key checks restored in "
                    f"{_format_elapsed(perf_counter() - fk_restore_started)}."
                )
            if indices:
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
                            logger.error(f"Table `{table_name}`: Failed to restore {idx.name}: {e}")
                    else:
                        logger.debug(f"Table `{table_name}`: Index {idx.name} already exists on disk. Skipping.")
                logger.info(
                    f"Table `{table_name}`: Index verification/rebuild completed in "
                    f"{_format_elapsed(perf_counter() - rebuild_started)}."
                )


    @classmethod
    def get_staging_table(
        cls: Type[FileTableProtocol],
        session: so.Session,
        *,
        staging_schema_tag: str | None = None,
    ) -> sa.Table:
        """
        Return the reflected staging table.

        Parameters
        ----------
        session
            An active SQLAlchemy session bound to an engine.
        staging_schema_tag
            schema_translate_map tag the staging table lives in. ``None``
            means no schema qualification (backend-default behavior).

        Returns
        -------
        sqlalchemy.Table
            The reflected staging table.

        Raises
        ------
        sqlalchemy.exc.NoSuchTableError
            The staging table has not been created.

        Notes
        -----
        Reflects via ``session.connection()``, not the bare engine. On
        SQLite's SingletonThreadPool a second connection opened straight
        from the engine is the same underlying DBAPI connection, and closing
        that second wrapper resets its perceived transaction state, silently
        discarding the session's own uncommitted work.
        """
        _require_bind(session, cls)
        backend = resolve_backend(session, staging_schema_tag=staging_schema_tag, mapper=cls)
        return sa.Table(
            backend.staging_name_for_table(cls.__tablename__),
            sa.MetaData(),  # throwaway — keeps staging table out of Base.metadata
            autoload_with=session.connection(bind_arguments={"mapper": cls}),
            schema=backend.staging_schema,
        )

    @classmethod
    def load_staging(
        cls: Type[FileTableProtocol],
        loader_context: LoaderContext,
        reader: FileReader,
    ) -> int:
        """
        Create the staging table and load the file into it.

        Parameters
        ----------
        loader_context
            Context object containing session, path, and load options.
        reader
            Reader that yields the file as Arrow batches.

        Returns
        -------
        int
            Number of rows loaded into the staging table.
        """
        session, tag = loader_context.session, loader_context.staging_schema_tag
        _require_bind(session, cls)
        left_out = set() if reader.empty else cls.autoincrement_columns() - set(reader.columns)
        cls.create_staging_table(session, staging_schema_tag=tag, without=left_out)
        staging = cls.get_staging_table(session, staging_schema_tag=tag)
        return stage_file(loader_context, reader, staging)

    @classmethod
    def _select_reader(cls: Type[FileTableProtocol], ctx: LoaderContext) -> FileReader:
        """
        Select the reader for a file. Override to read other formats.

        The default picks the reader from the file suffix: Parquet for
        ``.parquet`` and delimited text for every other suffix.

        Parameters
        ----------
        ctx
            Context of the load.

        Returns
        -------
        FileReader
            Reader for the file.
        """
        return open_reader(
            ctx.path,
            ctx.tableclass.model_columns(),
            nulls=ctx.policy.nulls,
            quote_mode=ctx.quote_mode,
            chunksize=ctx.chunksize,
            skip_malformed=ctx.policy.errors.on_error == "reject_row",
        )

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
        quote_mode: str = "by_delimiter",
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
            Reader for the file. The table's ``_select_reader`` chooses one
            when omitted, and ``chunksize`` and ``quote_mode`` then configure it.
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
            ``insert_if_empty``).
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

        if merge_strategy == "insert_if_empty":
            logger.info(
                f"Table `{cls.__tablename__}`: Checking whether target table is empty before staging load."
            )
            check_started = perf_counter()
            has_rows = cls._target_has_rows(session=session)
            logger.info(
                f"Table `{cls.__tablename__}`: Pre-load empty-table check completed in "
                f"{_format_elapsed(perf_counter() - check_started)}."
            )

            if has_rows:
                raise ValueError(
                    f"Table `{cls.__tablename__}` is not empty; cannot use merge strategy "
                    f"'insert_if_empty'"
                )

        if merge_strategy == "replace":
            backend = resolve_backend(session, staging_schema_tag=staging_schema_tag, mapper=cls)
            if backend.resolve_index_strategy(index_strategy) == "keep":
                backend._require_capability(
                    "supports_replace_with_kept_indexes",
                    "merge_strategy='replace' with index_strategy='keep'",
                )

        loader_context = LoaderContext(
            tableclass=cls,
            session=session,
            path=path,
            chunksize=chunksize,
            normalise=normalise,
            dedupe=dedupe,
            quote_mode=quote_mode,
            staging_schema_tag=staging_schema_tag,
            policy=policy or ConversionPolicy(),
            rejects_dir=rejects_dir,
            report=report or LoadReport(),
        )

        try:
            # Load to staging (Indices are already excluded via updated create_staging_table)
            logger.info(f"Table `{cls.__tablename__}`: Loading data into staging table")
            total = cls.load_staging(loader_context, reader or cls._select_reader(loader_context))

            # Merge staging to target (Wrapped in our index dropper!)
            logger.info(f"Table `{cls.__tablename__}`: Merging staging data into target table")
            with cls.manage_indices(session, index_strategy=index_strategy, staging_schema_tag=staging_schema_tag):
                cls.merge_from_staging(
                    session,
                    merge_strategy=merge_strategy,
                    merge_batch_size=merge_batch_size,
                    staging_schema_tag=staging_schema_tag,
                    validate_deferred_fks=validate_deferred_fks,
                    resolved=resolved,
                    physical_schemas=physical_schemas,
                )
                if validate_deferred_fks:
                    logger.info(f"Table `{cls.__tablename__}`: Validating deferred foreign keys.")
                    backend = resolve_backend(
                        session,
                        staging_schema_tag=staging_schema_tag,
                        mapper=cls,
                        resolved=resolved,
                        physical_schemas=physical_schemas,
                    )
                    backend.validate_deferred_fks(cls, session)
        finally:
            cls.drop_staging_table(session, staging_schema_tag=staging_schema_tag)

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
        merge_batch_size: int | None = None,
        staging_schema_tag: str | None = None,
        validate_deferred_fks: bool = True,
        resolved: ResolvedDatabase | None = None,
        physical_schemas: Mapping[str, str | None] | None = None,
    ):
        """
        Merge data from the staging table into the target table.

        Parameters
        ----------
        session
            An active SQLAlchemy session.
        merge_strategy
            Merge strategy to apply (for example ``replace``,
            ``upsert``, or ``insert_if_empty``).
        staging_schema_tag
            schema_translate_map tag the staging table lives in. ``None``
            means no schema qualification (backend-default behavior).
        validate_deferred_fks
            Forwarded to the backend's merge methods, which validate
            deferred FKs before every intermediate commit a batched merge
            makes. See ``load_file()``.
        resolved, physical_schemas
            Forwarded to ``resolve_backend()``. See ``load_file()``.
        """
        target = cls.__tablename__
        pk_cols = cls.pk_names()

        _require_bind(session, cls)
        backend = resolve_backend(
            session,
            staging_schema_tag=staging_schema_tag,
            mapper=cls,
            resolved=resolved,
            physical_schemas=physical_schemas,
        )
        target_empty_confirmed = False
        if merge_strategy in {"replace", "upsert"}:
            logger.info(
                f"Table `{target}`: Checking whether target table is empty for merge optimisation."
            )
            check_started = perf_counter()
            has_rows = cls._target_has_rows(session=session)
            logger.info(
                f"Table `{target}`: Empty-table optimisation check completed in "
                f"{_format_elapsed(perf_counter() - check_started)}."
            )
            if not has_rows:
                logger.info(
                    f"Table `{target}`: Target table is empty; routing merge strategy "
                    f"`{merge_strategy}` to insert-if-empty fast path."
                )
                target_empty_confirmed = True
                merge_strategy = "insert_if_empty"

        if merge_strategy == "replace":
            logger.info(f"Table `{target}`: Merge replace delete phase starting.")
            delete_started = perf_counter()
            backend.merge_replace(
                cls, session, pk_cols, merge_batch_size=merge_batch_size, validate_deferred_fks=validate_deferred_fks
            )
            logger.info(
                f"Table `{target}`: Merge replace delete phase completed in "
                f"{_format_elapsed(perf_counter() - delete_started)}."
            )
            logger.info(f"Table `{target}`: Merge insert phase starting.")
            insert_started = perf_counter()
            backend.merge_insert(
                cls, session, merge_batch_size=merge_batch_size, validate_deferred_fks=validate_deferred_fks
            )
            logger.info(
                f"Table `{target}`: Merge insert phase completed in "
                f"{_format_elapsed(perf_counter() - insert_started)}."
            )
        elif merge_strategy == "upsert":
            logger.info(f"Table `{target}`: Merge upsert phase starting.")
            upsert_started = perf_counter()
            backend.merge_upsert(
                cls, session, pk_cols, merge_batch_size=merge_batch_size, validate_deferred_fks=validate_deferred_fks
            )
            logger.info(
                f"Table `{target}`: Merge upsert phase completed in "
                f"{_format_elapsed(perf_counter() - upsert_started)}."
            )
        elif merge_strategy == "insert_if_empty":
            if not target_empty_confirmed:
                logger.info(f"Table `{target}`: Checking whether target table is empty.")
                check_started = perf_counter()
                has_rows = cls._target_has_rows(session=session)
                logger.info(
                    f"Table `{target}`: Empty-table check completed in "
                    f"{_format_elapsed(perf_counter() - check_started)}."
                )

                if has_rows:
                    raise ValueError(
                        f"Table `{target}` is not empty; cannot use merge strategy "
                        f"'insert_if_empty'"
                    )

            logger.info(f"Table `{target}`: Merge insert-if-empty phase starting.")
            insert_started = perf_counter()
            backend.merge_insert(
                cls, session, merge_batch_size=merge_batch_size, validate_deferred_fks=validate_deferred_fks
            )
            logger.info(
                f"Table `{target}`: Merge insert-if-empty phase completed in "
                f"{_format_elapsed(perf_counter() - insert_started)}."
            )
        else:
            raise ValueError(f"Unknown merge strategy '{merge_strategy}'")
    
    @classmethod
    def drop_staging_table(
        cls: Type[FileTableProtocol],
        session: so.Session,
        *,
        staging_schema_tag: str | None = None,
    ):
        """
        Drop the staging table if it exists.

        Parameters
        ----------
        session
            An active SQLAlchemy session bound to an engine.
        staging_schema_tag
            schema_translate_map tag the staging table lives in. ``None``
            means no schema qualification (backend-default behavior).
        """
        backend = resolve_backend(session, staging_schema_tag=staging_schema_tag, mapper=cls)
        backend.drop_staging_table(cls, session)
