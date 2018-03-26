import logging
from zmq.error import ZMQError
import zmq.green as zmq
from typing import Mapping, Generator, Tuple, Dict, Any

from cxworker.containers.adapters import DockerAdapter, BareAdapter, ContainerAdapter
from cxworker.errors import ContainerConfigurationError
from cxworker.containers.testing import DummyContainerAdapter
from .errors import ContainerError
from ..api.errors import UnknownContainerError
from .config import RegistryConfig


class Container:
    """
    A holder for information about a container that are not dependent on the exact implementation of the container
    runtime (e.g. Docker).
    """

    def __init__(self, socket: zmq.Socket, container_type, adapter: ContainerAdapter):
        self.socket = socket
        self.current_request = None
        self.master = None
        self.model_name = None
        self.model_version = None
        self.container_type = container_type
        self.adapter = adapter

    def set_model(self, name, version):
        self.model_name = name
        self.model_version = version


class ContainerRegistry:
    """
    Manages creation and access to a configured set of containers
    """

    def __init__(self, zmq_context: zmq.Context, registry: RegistryConfig,
                 container_config: Mapping[str, Dict[str, Any]]):
        self.poller = zmq.Poller()
        self.containers: Dict[str, Container] = {}
        self.container_config = container_config
        self.registry = registry

        for container_id, config in container_config.items():
            socket = zmq_context.socket(zmq.DEALER)
            container_type = config.get("type", None)

            if container_type is None:
                raise ContainerConfigurationError("No type specified for container '{}'".format(container_id))

            if container_type == "docker":
                adapter = DockerAdapter(config, registry)
            elif container_type == "bare":
                adapter = BareAdapter(config)
            elif container_type == "dummy":
                adapter = DummyContainerAdapter(config)
            else:
                raise ContainerConfigurationError("Unknown container type: {}".format(container_type))

            logging.info('Creating container `%s` of type `%s`', container_id, container_type)
            self.containers[container_id] = Container(socket, container_type, adapter)
            self.poller.register(socket, zmq.POLLIN)

    def __getitem__(self, container_id: str) -> Container:
        """
        Get the container with the given ``container_id`.

        :param container_id: container id
        :return: container with the given ``container_id``
        :raise UnknownContainerError: if the given ``container_id`` is not known to this registry
        """
        if container_id not in self.containers:
            raise UnknownContainerError('Unknown container id `{}`'.format(container_id))
        return self.containers[container_id]

    def start_container(self, container_id: str, model: str, version: str, slaves: Tuple[str, ...] = ()):
        logging.info('Starting container `%s` with model `%s:%s`', container_id, model, version)
        container = self[container_id]

        for slave_id in slaves:
            slave_container = self[slave_id]
            if slave_container.container_type != container.container_type:
                message = "The type of slave container {slave_id} ({slave_type}) " \
                          "is different from the type of the master ({master_type})"\
                    .format(slave_id=slave_id, slave_type=slave_container.container_type,
                            master_type=container.container_type)

                raise RuntimeError(message)

        if container.adapter.running:
            self.kill_container(container_id)

        container.adapter.load_model(model, version)
        container.set_model(model, version)

        for slave_id in slaves:
            slave_container = self[slave_id]
            if slave_container.adapter.running:
                self.kill_container(slave_id)

            slave_container.master = container

        container.adapter.start(slaves)
        container.socket.connect("tcp://0.0.0.0:{}".format(container.adapter.config.port))

    def refresh_model(self, container_id: str):
        container = self[container_id]

        # If there is an update, restart the container
        if container.adapter.update_model():
            current_slaves = tuple(container_id for container_id, container in self.containers.items()
                                   if container.master == container)
            self.start_container(container_id, container.model_name, container.model_version, current_slaves)

    def kill_container(self, container_id: str):
        logging.info('Killing container `%s`', container_id)
        container = self[container_id]
        zmq_address = "tcp://0.0.0.0:{}".format(container.adapter.config.port)
        try:
            container.socket.disconnect(zmq_address)
        except ZMQError:
            logging.warning('Failed to disconnect socket of `{}`  (perhaps it was not started/connected)'
                            .format(container_id))
        container.adapter.kill()

    def send_input(self, container_id: str, request_metadata, input: bytes):
        container = self[container_id]
        container.current_request = request_metadata
        container.socket.send_multipart([b"input", input])

    def wait_for_output(self) -> Generator[str, None, None]:
        """
        Wait until output arrives from one or more containers.
        :return: a generator of ids of containers from which we received output
        """

        result = self.poller.poll()
        return (container_id for container_id, container in self.containers.items()
                if (container.socket, zmq.POLLIN) in result)

    def read_output(self, container_id: str) -> str:
        message_type, message, *rest = self[container_id].socket.recv_multipart()

        if message_type == b"output":
            return message
        elif message_type == b"error":
            if len(rest) >= 1:
                logging.error("Received error traceback:")
                logging.error(rest[0])
            raise ContainerError("The container encountered an error: " + message.decode())
        else:
            raise ContainerError("The container responded with an unknown message type " + message_type.decode())

    def get_status(self) -> Generator[dict, None, None]:
        """
        Get status information for all containers
        :return: a generator of status information
        """

        for name, container in self.containers.items():
            yield {
                "name": name,
                "running": container.adapter.running,
                "request": container.current_request.id if container.current_request is not None else None,
                "model_name": container.model_name,
                "model_version": container.model_version
            }

    def kill_all(self):
        for name, container in self.containers.items():
            if container.adapter.running is not None:
                self.kill_container(name)

    def get_current_request(self, container_id):
        return self[container_id].current_request

    def request_finished(self, container_id):
        self[container_id].current_request = None
