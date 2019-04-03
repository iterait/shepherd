from apistrap.errors import ApiClientError, ApiServerError


__all__ = ['ApiServerError', 'ApiClientError', 'UnknownSheepError', 'UnknownJobError', 'StorageError',
           'StorageInaccessibleError', 'NameConflictError']


class UnknownSheepError(ApiClientError):
    """Exception raised when application attempts to use a sheep with an unknown id."""


class UnknownJobError(ApiClientError):
    """Exception raised when a client asks about a job that is not assigned to this shepherd."""


class StorageError(ApiServerError):
    """Exception raised when application encounters some issue with the minio storage."""


class StorageInaccessibleError(ApiServerError):
    """Exception raised when the remote storage is not accessible at the moment"""


class NameConflictError(ApiClientError):
    """Exception raised when a client chooses a job ID that was already used"""
