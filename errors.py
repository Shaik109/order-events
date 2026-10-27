"""Exception types. NonRetryableError marks failures that a retry cannot fix."""


class NonRetryableError(Exception):
    """Base class for failures the orchestrator must not retry."""


class SourceError(NonRetryableError):
    """A source file is missing, has an unexpected schema, or holds no rows."""


class MissingSourceError(SourceError):
    """An expected source file does not exist."""


class SchemaChangeError(SourceError):
    """The header of a source file differs from the declared schema."""


class EmptyBatchError(SourceError):
    """A source file contains a header but no data rows."""


class ReconciliationError(NonRetryableError):
    """Post-load checks or row accounting failed. Publishing must not happen."""
