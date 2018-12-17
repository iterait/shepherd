import json
import sys, inspect
from schematics import Model
from schematics.types import StringType, serializable, PolyModelType
from typing import Iterable, Optional


class Message(Model):
    """Base zmq socket message."""

    identity = StringType(default='')
    """Optional identity (for zmq ROUTER sockets)."""

    job_id = StringType()
    """**shepherd** job id."""

    @serializable
    def message_type(self):
        """Human readable message type."""
        return self.__class__.__name__


def get_message_classes() -> Iterable[type]:
    """Find and return all classes inheriting from :py:class:`Message` in this module."""
    return inspect.getmembers(sys.modules[__name__], lambda obj: inspect.isclass(obj) and issubclass(obj, Message))


def claim(field, data) -> Optional[type]:
    """Return class from this module which ``message_type`` is equal to ``data["message_type"]``."""
    for name, cls in get_message_classes():
        if data["message_type"] == name:
            return cls


class InputMessage(Message):
    """Message informing the runner about a job being ready to be processed."""

    io_data_root = StringType()
    """Job data root (with ``inputs`` and ``outputs`` folders)."""


class DoneMessage(Message):
    """Message informing :py:class:`shepherd.shepherd.Shepherd` about a finished job."""
    pass


class ErrorMessage(Message):
    """Message informing :py:class:`shepherd.shepherd.Shepherd` about an encountered error."""

    short_error = StringType()
    """Human-readable short error message."""

    long_error = StringType()
    """Longer error message (e.g.: stacktrace)."""


class MessageWrapper(Model):
    """Message wrapper allowing simple en/de-coding."""

    message = PolyModelType(tuple(map(lambda item: item[1], get_message_classes())), claim_function=claim)
    """Wrapped message (inheriting from :py:class:`Message`)."""


def encode_message(message: Message) -> bytes:
    """Encode the given message to bytes which may be send through zmq socket."""
    message.validate()
    wrapper = MessageWrapper(dict(message=message))
    return json.dumps(wrapper.to_primitive()).encode()


def decode_message(value: bytes) -> Message:
    """Decode the given bytes array received from zmq socket to a message."""
    wrapper = MessageWrapper(json.loads(value.decode()))
    wrapper.validate()
    return wrapper.message
