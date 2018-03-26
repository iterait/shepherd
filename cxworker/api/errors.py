class AppError(Exception, BaseException):
    """Base application exception, will result in 500 response."""


class ClientActionError(AppError):
    """Base client-side error exception, will result in 400 response."""


class UnknownContainerError(ClientActionError):
    """Exception raised when application attempts to use container with unknown id."""


class StorageError(AppError):
    """Exception raised when application encounters some issue with the minio storage."""
