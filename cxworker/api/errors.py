class AppError(Exception, BaseException):
    pass


class ClientActionError(AppError):
    pass


class UnknownContainerError(ClientActionError):
    pass


class StorageError(Exception):
    pass
