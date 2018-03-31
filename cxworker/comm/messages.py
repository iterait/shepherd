import json
import sys, inspect
from schematics import Model
from schematics.types import StringType, serializable, PolyModelType
from typing import List


class Message(Model):
    identity = StringType(default='')
    job_id = StringType()

    @serializable
    def message_type(self):
        return self.__class__.__name__


def get_message_classes():
    return inspect.getmembers(sys.modules[__name__], lambda obj: inspect.isclass(obj) and issubclass(obj, Message))


def claim(field, data):
    for name, cls in get_message_classes():
        if data["message_type"] == name:
            return cls

    return None


class InputMessage(Message):
    io_data_root = StringType()


class DoneMessage(Message):
    pass


class ErrorMessage(Message):
    short_error = StringType()
    long_error = StringType()


class MessageWrapper(Model):
    message = PolyModelType(tuple(map(lambda item: item[1], get_message_classes())), claim_function=claim)


def encode_message(message: Message) -> List[bytes]:
    message.validate()
    wrapper = MessageWrapper(dict(message=message))
    return [json.dumps(wrapper.to_primitive()).encode()]


def decode_message(value: bytes) -> Message:
    wrapper = MessageWrapper(json.loads(value.decode()))
    wrapper.validate()
    return wrapper.message
