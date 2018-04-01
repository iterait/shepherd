import abc
import logging
from typing import List, Optional, Dict

import zmq.green as zmq
from zmq.error import ZMQBaseError
from gevent.queue import Queue
from schematics import Model
from schematics.types import StringType, IntType, ListType

from ..api.models import ModelModel


class BaseSheep(metaclass=abc.ABCMeta):
    """
    A base class for container adapters - classes that allow launching different kinds of containers.
    """

    class Config(Model):
        type: str = StringType(required=True)
        port: int = IntType(required=True)
        devices: List[str] = ListType(StringType, default=lambda: [])

    _config: Config

    def __init__(self, socket: zmq.Socket, sheep_data_root: str):
        """
        Create new :py:class:`BaseSheep`.

        :param socket: socket for feeding sheep's runner with InputMessages
        :param sheep_data_root: sheep data root with job working directories
        """
        self._config: Optional[self.Config] = None
        self.socket: zmq.Socket = socket
        self.jobs_queue: Queue = Queue()  # queue of jobs to be processed
        self.jobs_meta: Dict[str, ModelModel] = dict()  # mapping from job_id to job meta (model name/version)
        self.model_name: Optional[str] = None  # current model name
        self.model_version: Optional[str] = None  # current model version
        self.sheep_data_root: Optional[str] = sheep_data_root
        self.in_progress: set = set()  # set of job_ids which are currently sent for processing to the sheep's runner

    def _load_model(self, model_name: str, model_version: str) -> None:
        """Tell the sheep to prepare a new model (without restarting)."""
        self.model_name = model_name
        self.model_version = model_version

    def start(self, model_name: str, model_version: str) -> None:
        """
        (Re)start the sheep with the given model name and version.
        Any unfinished jobs will be lost, socket connection will be reset.

        :param model_name: model name
        :param model_version: model version
        """
        if self.running:
            self.slaughter()
        self._load_model(model_name, model_version)
        self.in_progress = set()
        self.socket.connect("tcp://0.0.0.0:{}".format(self._config.port))

    def slaughter(self) -> None:
        zmq_address = "tcp://0.0.0.0:{}".format(self._config.port)
        try:
            self.socket.disconnect(zmq_address)
        except ZMQBaseError:
            logging.warning('Failed to disconnect socket (perhaps it was not started/connected)')

    @property
    @abc.abstractmethod
    def running(self) -> bool:
        """Is the sheep running, i.e. capable of accepting computation requests?"""
