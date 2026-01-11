class IngestError(Exception):
    """Raised when ingestion fails for structural or runtime reasons."""


class ValidationError(Exception):
    """Raised when schema or specification validation fails."""
