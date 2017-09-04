import abc
from typing import Iterable


class BaseResponse(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def dump(self) -> dict:
        pass


class ErrorResponse(BaseResponse):
    def __init__(self, message: str):
        self.message = message

    def dump(self) -> dict:
        return {
            "message": self.message
        }


class StatusResponse(BaseResponse):
    def __init__(self, containers_status: Iterable[dict]):
        self.containers_status = list(containers_status)

    def dump(self) -> dict:
        return {
            data['name']: {
                'running': data['running'],
                'request': data['request'],
                'model': {
                    'name': data['model_name'],
                    'version': data['model_version']
                }
            } for data in self.containers_status
        }


class InterruptJobResponse(BaseResponse):
    def dump(self) -> dict:
        return {}


class StartJobResponse(BaseResponse):
    def dump(self) -> dict:
        return {}


class ReconfigureResponse(BaseResponse):
    def dump(self) -> dict:
        return {}
