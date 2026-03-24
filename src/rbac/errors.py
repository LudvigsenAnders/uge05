class ProvisioningError(Exception):
    """Base class for provisioning errors."""


class DatabaseConnectionError(ProvisioningError):
    """Connection issues."""


class SQLExecutionError(ProvisioningError):
    """SQL failure."""


class AuditLoggingError(ProvisioningError):
    """Audit logging failed."""
