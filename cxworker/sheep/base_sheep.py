import abc
import logging
from typing import List, Optional, DefaultDict

import zmq.green as zmq
from zmq.error import ZMQBaseError
from gevent.queue import Queue
from schematics import Model
from schematics.types import StringType, IntType, ListType

from ..api.models import ModelModel


__all__ = ['BaseSheep']


class BaseSheep(metaclass=abc.ABCMeta):
    """
    A base class for container adapters - classes that allow launching different kinds of containers.
    """

    class Config(Model):
        type: str = StringType(required=True)
        port: int = IntType(required=True)
        devices: List[str] = ListType(StringType, default=lambda: [])

    config: Config

    def __init__(self, socket: zmq.Socket, sheep_data_root: str):
        self.config: Optional[self.Config] = None
        self.socket: zmq.Socket = socket
        self.jobs_queue: Queue = Queue()
        self.jobs_meta: DefaultDict[str, ModelModel] = dict()
        self.model_name: Optional[str] = None
        self.model_version: Optional[str] = None
        self.sheep_data_root: Optional[str] = sheep_data_root
        self.in_progress: set = set()

    def load_model(self, model_name: str, model_version: str):
        """
        Tell the sheep to prepare a new model (without restarting).
        """
        self.model_name = model_name
        self.model_version = model_version

    @abc.abstractmethod
    def update_model(self) -> bool:
        """
        Update the currently loaded model. The underlying process/container/etc. should not be restarted.
        :return: True if there was an update, False otherwise
        """

    def start(self, model_name: str, model_version: str):
        logging.info('Starting sheep with model `%s:%s`', model_name, model_version)

        if self.running:
            self.slaughter()

        self.load_model(model_name, model_version)
        self.in_progress = set()
        self.socket.connect("tcp://0.0.0.0:{}".format(self.config.port))

    def slaughter(self):
        zmq_address = "tcp://0.0.0.0:{}".format(self.config.port)
        try:
            self.socket.disconnect(zmq_address)
        except ZMQBaseError:
            logging.warning('Failed to disconnect socket of (perhaps it was not started/connected)')

    @property
    @abc.abstractmethod
    def running(self) -> bool:
        """
        Is the sheep running, i.e. capable of accepting computation requests?
        """
