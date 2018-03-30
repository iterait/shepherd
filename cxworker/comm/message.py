from abc import ABCMeta, abstractmethod, abstractstaticmethod
from typing import List, Optional, Union, Sequence

import zmq.green as zmq
from zmq.error import ZMQBaseError


__all__ = ['Messenger', 'InputMessage', 'DoneMessage', 'ErrorMessage', 'MessengerError', 'UnknownMessageTypeError']


class MessengerError(ValueError):
    pass


class UnknownMessageTypeError(MessengerError):
    pass


class UnexpectedMessageTypeError(MessengerError):
    pass


class BaseMessage(metaclass=ABCMeta):

    def __init__(self, identity: Optional[bytes]=None):
        self.identity: Optional[bytes] = identity

    @abstractmethod
    def serialize(self) -> List[bytes]:
        pass

    @staticmethod
    @abstractstaticmethod
    def type() -> str:
        pass


class InputMessage(BaseMessage):

    def __init__(self, job_id: str, io_data_root: str, **kwargs):
        super().__init__(**kwargs)
        self.job_id: str = job_id
        self.io_data_root: str = io_data_root

    def serialize(self) -> List[bytes]:
        return [self.job_id.encode(), self.io_data_root.encode()]

    @staticmethod
    def type() -> str:
        return 'input'


class DoneMessage(BaseMessage):

    def __init__(self, job_id: str, **kwargs):
        super().__init__(**kwargs)
        self.job_id: str = job_id

    def serialize(self) -> List[bytes]:
        return [self.job_id.encode()]

    @staticmethod
    def type() -> str:
        return 'output'


class ErrorMessage(BaseMessage):

    def __init__(self, job_id: str, short_error: str, long_error: Optional[str]=None, **kwargs):
        super().__init__(**kwargs)
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
    Static helper class for sending and receiving images through zmq DEALER/ROUTER sockets.
    """

    _MESSAGE_TYPES_MAPPING = {InputMessage.type(): InputMessage,
                              DoneMessage.type(): DoneMessage,
                              ErrorMessage.type(): ErrorMessage}

    @staticmethod
    def send(socket: zmq.Socket, message: BaseMessage, response_to: Optional[BaseMessage]=None) -> None:
        """
        Encode the given image and send it to the given DEALER/ROUTER socket.

        :param socket: socket to send the message to
        :param message: message to be send
        :param response_to: optional message to respond to
        :raise MessengerError: if it fails
        :raise UnknownMessageTypeError: if the message to be send is of unknown type
        """
        assert socket.type in [zmq.DEALER, zmq.ROUTER]

        # check if the message type is known
        if type(message) not in Messenger._MESSAGE_TYPES_MAPPING.values():
            raise UnknownMessageTypeError('Unknown message type `{}`. Known message types are {}.'
                                          .format(str(type(message)), list(Messenger._MESSAGE_TYPES_MAPPING.keys())))

        # serialize and send the message
        serialized_message = [message.type().encode()] + message.serialize()
        if response_to is not None and response_to.identity is not None:
            serialized_message = [response_to.identity] + serialized_message
        try:
            socket.send_multipart(serialized_message)
        except ZMQBaseError as zmq_error:
            raise MessengerError('Failed to send message') from zmq_error

    @staticmethod
    def recv(socket: zmq.Socket, expected_message_types: Optional[Sequence[type]]=None) \
            -> Union[InputMessage, DoneMessage, ErrorMessage]:
        """
        Receive, decode and return a message from the given DEALER/ROUTER socket.

        :param socket: socket to receive the message from
        :param expected_message_types: a sequence of expected message types (optional)
        :raise MessengerError: if receiving fails
        :raise UnknownMessageTypeError: if the received message is of unknown type
        :raise UnexpectedMessageTypeError: if the received message type is not expected
        """
        assert socket.type in [zmq.DEALER, zmq.ROUTER]

        # receive the message
        try:
            if socket.type == zmq.DEALER:
                identity = None
                message_type, *message_parts = socket.recv_multipart()
            elif socket.type == zmq.ROUTER:
                identity, message_type, *message_parts = socket.recv_multipart()
            message_type = message_type.decode()
            message_parts = [part.decode() for part in message_parts]
        except ZMQBaseError as zmq_error:
            raise MessengerError('Failed to receive message') from zmq_error

        # check if message type is known
        if message_type not in Messenger._MESSAGE_TYPES_MAPPING:
            raise UnknownMessageTypeError('Unknown message type `{}`. Known message types are {}.'
                                          .format(message_type, list(Messenger._MESSAGE_TYPES_MAPPING.keys())))

        # check if message type is expected
        message_class = Messenger._MESSAGE_TYPES_MAPPING[message_type]
        if expected_message_types is not None and message_class not in expected_message_types:
            raise UnexpectedMessageTypeError('Unexpected message type `{}`. Expected message types are {}.'
                                             .format(message_type, expected_message_types))
        # create and return the message
        return message_class(*message_parts, identity=identity)
