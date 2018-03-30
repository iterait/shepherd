from abc import ABCMeta, abstractmethod, abstractstaticmethod
from typing import List, Optional


__all__ = ['BaseMessage', 'InputMessage', 'DoneMessage', 'ErrorMessage']


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
