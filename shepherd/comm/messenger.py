from typing import Union, Sequence

import zmq
import zmq.asyncio
from zmq.error import ZMQBaseError

from ..errors.comm import MessageError, UnexpectedMessageTypeError
from .messages import *


class Messenger:
    """
    Static helper class for sending and receiving messages through zmq sockets.
    """

    @staticmethod
    async def send(socket: zmq.asyncio.Socket, message: Message, response_to: Optional[Message]=None) -> None:
        """
        Encode given message and send it to the given socket.

        :param socket: socket to send the message to
        :param message: message to be send
        :param response_to: optional message to respond to
        :raise MessengerError: if it fails
        :raise UnknownMessageTypeError: if the message to be send is of unknown type
        """
        if not isinstance(message, Message):
            raise TypeError('`{}` is not a message'.format(str(type(message))))

        # serialize and send the message
        serialized_message = [encode_message(message)]
        if response_to is not None and response_to.identity != '':
            serialized_message = [response_to.identity] + serialized_message
        try:
            await socket.send_multipart(serialized_message)
        except ZMQBaseError as zmq_error:
            raise MessageError('Failed to send message') from zmq_error

    @staticmethod
    async def recv(socket: zmq.asyncio.Socket, expected_message_types: Optional[Sequence[type]]=None,
                   noblock: bool=False) -> Union[InputMessage, DoneMessage, ErrorMessage]:
        """

        Receive, decode and return a message from the given socket.

        :param socket: socket to receive the message from
        :param expected_message_types: a sequence of expected message types (optional)
        :param noblock: do not block on ``recv`` call and throw if there is no message
        :raise MessengerError: if receiving fails
        :raise UnknownMessageTypeError: if the received message is of unknown type
        :raise UnexpectedMessageTypeError: if the received message type is not expected
        """
        # receive the message
        try:
            identity = ''
            if socket.type == zmq.ROUTER:
                identity, message = await socket.recv_multipart(flags=zmq.NOBLOCK if noblock else 0)
            else:
                message, = await socket.recv_multipart(flags=zmq.NOBLOCK if noblock else 0)
            message = decode_message(message)
            message.identity = identity
        except ZMQBaseError as zmq_error:
            raise MessageError('Failed to receive message') from zmq_error

        # check if message type is expected
        if expected_message_types is not None and type(message) not in expected_message_types:
            raise UnexpectedMessageTypeError('Unexpected message type `{}`. Expected message types are {}.'
                                             .format(message.message_type, expected_message_types))
        # create and return the message
        return message
