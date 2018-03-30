from typing import Optional, Union, Sequence

import zmq.green as zmq
from zmq.error import ZMQBaseError

from .errors import *
from .messages import *


class Messenger:
    """
    Static helper class for sending and receiving messages through zmq sockets.
    """

    _MESSAGE_TYPES_MAPPING = {InputMessage.type(): InputMessage,
                              DoneMessage.type(): DoneMessage,
                              ErrorMessage.type(): ErrorMessage}

    @staticmethod
    def send(socket: zmq.Socket, message: BaseMessage, response_to: Optional[BaseMessage]=None) -> None:
        """
        Encode the given image and send it to the given socket.

        :param socket: socket to send the message to
        :param message: message to be send
        :param response_to: optional message to respond to
        :raise MessengerError: if it fails
        :raise UnknownMessageTypeError: if the message to be send is of unknown type
        """
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
            raise MessageError('Failed to send message') from zmq_error

    @staticmethod
    def recv(socket: zmq.Socket, expected_message_types: Optional[Sequence[type]]=None) \
            -> Union[InputMessage, DoneMessage, ErrorMessage]:
        """
        Receive, decode and return a message from the given socket.

        :param socket: socket to receive the message from
        :param expected_message_types: a sequence of expected message types (optional)
        :raise MessengerError: if receiving fails
        :raise UnknownMessageTypeError: if the received message is of unknown type
        :raise UnexpectedMessageTypeError: if the received message type is not expected
        """
        # receive the message
        try:
            if socket.type == zmq.ROUTER:
                identity, message_type, *message_parts = socket.recv_multipart()
            else:
                identity = None
                message_type, *message_parts = socket.recv_multipart()
            message_type = message_type.decode()
            message_parts = [part.decode() for part in message_parts]
        except ZMQBaseError as zmq_error:
            raise MessageError('Failed to receive message') from zmq_error

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
