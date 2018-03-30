from abc import ABCMeta, abstractmethod, abstractstaticmethod
from typing import List, Optional, Union

import zmq.green as zmq
from zmq.error import ZMQBaseError


__all__ = ['Messenger', 'InputMessage', 'DoneMessage', 'ErrorMessage', 'MessengerError', 'UnknownMessageTypeError']


class MessengerError(ValueError):
    pass


class UnknownMessageTypeError(MessengerError):
    pass


class BaseMessage(metaclass=ABCMeta):

    @abstractmethod
    def serialize(self) -> List[bytes]:
        pass

    @staticmethod
    @abstractstaticmethod
    def type() -> str:
        pass


class InputMessage(BaseMessage):

    def __init__(self, job_id: str, io_data_root: str):
        self.job_id: str = job_id
        self.io_data_root: str = io_data_root

    def serialize(self) -> List[bytes]:
        return [self.job_id.encode(), self.io_data_root.encode()]

    @staticmethod
    def type() -> str:
        return 'input'


class DoneMessage(BaseMessage):

    def __init__(self, job_id: str):
        self.job_id: str = job_id

    def serialize(self) -> List[bytes]:
        return [self.job_id.encode()]

    @staticmethod
    def type() -> str:
        return 'output'


class ErrorMessage(BaseMessage):

    def __init__(self, job_id: str, short_error: str, long_error: Optional[str]=None):
        self.job_id: str = job_id
        self.short_error: str = short_error
        self.long_error: str = long_error

    def serialize(self) -> List[bytes]:
        return [self.job_id.encode(), self.short_error.encode(), self.long_error.encode()]

    @staticmethod
    def type() -> str:
        return 'error'


class Messenger:
    """
    Static helper class for sending and receiving images through zmq sockets.
    """

    _MESSAGE_TYPES_MAPPING = {InputMessage.type(): InputMessage,
                              DoneMessage.type(): DoneMessage,
                              ErrorMessage.type(): ErrorMessage}

    @staticmethod
    def send(socket: zmq.Socket, message: Union[InputMessage, DoneMessage, ErrorMessage]) -> None:
        """
        Encode the given image and send it to the given socket.

        :param socket: socket to send the message to
        :param message: message to be send
        :raise MessengerError: if it fails
        :raise UnknownMessageTypeError: if the message to be send is of unknown type
        """
        if type(message) not in Messenger._MESSAGE_TYPES_MAPPING.values():
            raise UnknownMessageTypeError('Unknown message type `{}`. Known message types are {}.'
                                          .format(str(type(message)), list(Messenger._MESSAGE_TYPES_MAPPING.keys())))
        serialized_message = [message.type().encode()] + message.serialize()
        try:
            socket.send_multipart(serialized_message)
        except ZMQBaseError as zmq_error:
            raise MessengerError('Failed to send message') from zmq_error

    @staticmethod
    def recv(socket: zmq.Socket) -> Union[InputMessage, DoneMessage, ErrorMessage]:
        """
        Receive, decode and return a message from the given socket.

        :param socket: socket to receive the message from
        :raise MessengerError: if receiving fails
        :raise UnknownMessageTypeError: if the received message is of unknown type
        """
        try:
            message_type, *parts = socket.recv_multipart()
            parts = [part.decode() for part in parts]
        except ZMQBaseError as zmq_error:
            raise MessengerError('Failed to receive message') from zmq_error
        if message_type not in Messenger._MESSAGE_TYPES_MAPPING:
            raise UnknownMessageTypeError('Unknown message type `{}`. Known message types are {}.'
                                          .format(message_type, list(Messenger._MESSAGE_TYPES_MAPPING.keys())))
        return Messenger._MESSAGE_TYPES_MAPPING[message_type](*parts)
