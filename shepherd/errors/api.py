__all__ = ['AppError', 'ClientActionError', 'UnknownSheepError', 'UnknownJobError', 'StorageError',
           'StorageInaccessibleError', 'NameConflictError']


class AppError(Exception, BaseException):
    """Base application exception, will result in 500 response."""


class ClientActionError(AppError):
    """Base client-side error exception, will result in 400 response."""


class UnknownSheepError(ClientActionError):
    """Exception raised when application attempts to use a sheep with an unknown id."""


class UnknownJobError(ClientActionError):
    """Exception raised when a client asks about a job that is not assigned to this shepherd."""


class StorageError(AppError):
    """Exception raised when application encounters some issue with the minio storage."""


class StorageInaccessibleError(AppError):
    """Exception raised when the remote storage is not accessible at the moment"""


class NameConflictError(ClientActionError):
    """Exception raised when a client chooses a job ID that was already used"""
