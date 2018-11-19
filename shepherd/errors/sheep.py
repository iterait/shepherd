__all__ = ['SheepError', 'SheepConfigurationError']


class SheepError(BaseException):
    pass


class SheepConfigurationError(SheepError):
    pass
