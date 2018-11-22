__all__ = ['MessageError', 'UnexpectedMessageTypeError']


class MessageError(ValueError):
    pass


class UnexpectedMessageTypeError(MessageError):
    pass
