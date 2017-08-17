class AppError(Exception, BaseException):
    pass


class ClientActionError(AppError):
    pass
