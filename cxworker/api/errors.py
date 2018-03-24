class AppError(Exception, BaseException):
    pass


class ClientActionError(AppError):
    status_code = 400


class UnknownContainerError(ClientActionError):
    pass
