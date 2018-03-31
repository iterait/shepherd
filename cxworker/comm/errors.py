class MessageError(ValueError):
    pass


class UnknownMessageTypeError(MessageError):
    pass


class UnexpectedMessageTypeError(MessageError):
    pass
