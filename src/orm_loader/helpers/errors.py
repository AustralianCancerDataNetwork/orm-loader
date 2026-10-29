class IngestError(Exception):
    """Raised when ingestion fails for structural or runtime reasons."""


class ValidationError(Exception):
    """Raised when schema or specification validation fails."""


class ConversionError(IngestError):
    """Raised when values cannot be read as their column type.

    Parameters
    ----------
    table_name
        Table the column belongs to, if known.
    column_name
        Column the values were read for, if known.
    expected
        Name of the column type.
    count
        Number of unreadable values.
    samples
        First few unreadable raw values.
    """

    def __init__(
        self,
        *,
        table_name: str | None,
        column_name: str | None,
        expected: str,
        count: int,
        samples: list,
    ) -> None:
        self.table_name = table_name
        self.column_name = column_name
        self.expected = expected
        self.count = count
        self.samples = samples
        target = ".".join(part for part in (table_name, column_name) if part) or "column"
        hint = " Declare the source format with DateHandler(formats=...)." if "Date" in expected else ""
        super().__init__(
            f"{target}: {count} value(s) cannot be read as {expected}. Examples: {samples}.{hint}"
        )


class RequiredValueError(IngestError):
    """Raised when a required column has missing values.

    Parameters
    ----------
    table_name
        Table the column belongs to.
    column_name
        The required column.
    count
        Number of rows missing a value.
    rows
        First few data row numbers (starting at 1) that are missing a value.
    """

    def __init__(self, *, table_name: str, column_name: str, count: int, rows: list[int]) -> None:
        self.table_name = table_name
        self.column_name = column_name
        self.count = count
        self.rows = rows
        super().__init__(
            f"{table_name}.{column_name}: {count} row(s) have no value for a required column. "
            f"First data rows: {rows}."
        )


class RejectLimitError(IngestError):
    """Raised when rejected rows exceed the policy's ``max_rejects``.

    Parameters
    ----------
    rejected
        Number of rows rejected so far.
    read
        Number of rows read so far.
    limit
        The configured limit.
    """

    def __init__(self, *, rejected: int, read: int, limit: float) -> None:
        self.rejected = rejected
        self.read = read
        self.limit = limit
        super().__init__(f"{rejected} of {read} rows rejected, over the limit of {limit}.")
