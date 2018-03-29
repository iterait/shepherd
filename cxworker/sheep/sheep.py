import logging
import subprocess
import os
import abc
import re
import shlex
import os.path as path
import json
from zmq import Socket
from zmq.error import ZMQError

import gevent
import zmq
from gevent.queue import Queue
from typing import Dict, Any, List, Optional, Sequence

from schematics import Model
from schematics.types import StringType, IntType, ListType, BooleanType
from zmq import green as zmq

from cxworker.docker import DockerContainer, DockerImage
from cxworker.errors import SheepConfigurationError
from cxworker.manager.config import RegistryConfig


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
        self.socket = socket
        self.requests_queue = Queue()
        self.requests_set = set()
        self.model_name = None
        self.model_version = None
        self.sheep_data_root = sheep_data_root

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

    @abc.abstractmethod
    def start(self):
        """
        Start the underlying process/container/etc. and make it listen on the configured port.
        The load_model method must always be called before this.
        """

    @abc.abstractmethod
    def slaughter(self):
        """
        Forcefully terminate the underlying process/container/etc.
        """

    @property
    @abc.abstractmethod
    def running(self) -> bool:
        """
        Is the sheep running, i.e. capable of accepting computation requests?
        """


def extract_gpu_number(device_name: str) -> Optional[str]:
    """
    Extract GPU number from a Linux device name
    >>> extract_gpu_number("/dev/nvidia1")
    '1'
    >>> extract_gpu_number("/dev/sda2") is None
    True
    >>> extract_gpu_number("/dev/nvidiactl") is None
    True
    """

    match = re.match(r'/dev/nvidia([0-9]+)$', device_name)
    if match is not None:
        return match.group(1)
    return None


class DockerSheep(BaseSheep):
    """
    A container adapter with a Docker backend. If the user links nvidia devices into it, the nvidia runtime
    (a.k.a. nvidia-docker2) is used.
    """

    CONTAINER_PORT = 9999
    """Container port to bind the socket to."""

    class Config(BaseSheep.Config):
        autoremove_containers: bool = BooleanType(default=False)

    def __init__(self, config: Dict[str, Any], registry_config: RegistryConfig, **kwargs):
        super().__init__(**kwargs)
        self.config: self.Config = self.Config(config)
        self.registry_config = registry_config
        self.container: Optional[DockerContainer] = None
        self.image: Optional[DockerImage] = None

    def load_model(self, model_name: str, model_version: str):
        self.image = DockerImage(model_name, model_version, self.registry_config)
        self.image.pull()
        super().load_model(model_name, model_version)

    def update_model(self) -> bool:
        return self.image.pull()

    def start(self):
        devices = self.config.devices

        visible_gpu_numbers = list(filter(None, map(extract_gpu_number, devices)))
        env = {"NVIDIA_VISIBLE_DEVICES": ",".join(visible_gpu_numbers)}

        self.container = DockerContainer(self.image, self.config.autoremove_containers, env=env,
                                         runtime="nvidia" if visible_gpu_numbers else None)
        self.container.add_port_mapping(self.config.port, self.CONTAINER_PORT)
        self.container.add_bind_mount(self.sheep_data_root, self.sheep_data_root)
        self.container.start()

    def slaughter(self):
        if self.container is not None:
            self.container.kill()

    @property
    def running(self) -> bool:
        return self.container is not None and self.container.running


class BareSheep(BaseSheep):
    """
    An adapter that can only run one type of the model on bare metal.
    This might be useful when Docker isolation is impossible or not necessary, for example in deployments with just a
    few models.
    """

    class Config(BaseSheep.Config):
        model_name: str = StringType(required=True)
        model_version: str = StringType(required=True)
        config_path: str = StringType(required=True)
        working_directory: str = StringType(required=True)
        stdout_file: Optional[str] = StringType(required=False)
        stderr_file: Optional[str] = StringType(required=False)

    def __init__(self, config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config: self.Config = self.Config(config)
        self.process = None

    def load_model(self, model_name: str, model_version: str):
        if model_name != self.config.model_name:
            raise SheepConfigurationError("This sheep can only load model '{}'".format(model_name))

        if model_version != self.config.model_version:
            raise SheepConfigurationError("This sheep can only load version '{}' of model '{}'"
                                          .format(model_name, model_version))
        super().load_model(model_name, model_version)

    def update_model(self):
        pass

    def start(self):
        stdout = open(self.config.stdout_file, 'a') if self.config.stdout_file is not None else subprocess.DEVNULL
        stderr = open(self.config.stderr_file, 'a') if self.config.stderr_file is not None else subprocess.DEVNULL

        devices = self.config.devices

        env = os.environ.copy()
        env["CUDA_VISIBLE_DEVICES"] = ",".join(filter(None, map(extract_gpu_number, devices)))

        self.process = subprocess.Popen(
            shlex.split("cxworker-runner -p {} {}".format(self.config.port, self.config.config_path)), env=env,
            cwd=self.config.working_directory, stdout=stdout, stderr=stderr)

    def slaughter(self):
        if self.process is not None:
            self.process.kill()

    @property
    def running(self) -> bool:
        return self.process is not None and self.process.poll() is None


class DummySheep(BaseSheep):
    running = False

    def __init__(self, config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = self.Config(config)
        self.server = None
        self.feeding_socket: Socket = None

    def load_model(self, model_name: str, model_version: str):
        pass

    def update_model(self):
        pass

    def start(self):
        self.running = True
        if self.server is None:
            self.server = gevent.spawn(self.serve)

    def serve(self):
        context = zmq.Context()
        self.feeding_socket = context.socket(zmq.ROUTER)
        self.feeding_socket.setsockopt(zmq.LINGER, 0)
        address = f"tcp://*:{self.config.port}"
        self.feeding_socket.bind(address)

        logging.info('Testing container is listening at `%s`', address)
        while True:
            identity, message_type, job_id, io_path, *_ = self.feeding_socket.recv_multipart()
            job_id = job_id.decode()
            io_path = io_path.decode()
            logging.debug('Received job `%s` in `%s`', job_id, io_path)

            if message_type == b"input":
                gevent.sleep(3)
                input_path = path.join(io_path, job_id, 'inputs', 'input.json')
                payload = json.load(open(input_path))
                result_json = payload
                result_json['output'] = [payload['key'][0]*2]
                json.dump(result_json, open(path.join(io_path, job_id, 'outputs', 'output.json'), 'w'))
                self.feeding_socket.send_multipart([identity, b"output", job_id.encode()])
            else:
                logging.debug('Sending error `%s`')
                self.feeding_socket.send_multipart([identity, b"error", b"Unrecognized message type"])

    def slaughter(self):
        if self.server is not None:
            self.server.kill()
            self.server = None
        if self.feeding_socket is not None:
            self.feeding_socket.close(0)
            self.feeding_socket = None
        self.running = False
