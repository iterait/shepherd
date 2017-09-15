from typing import Mapping, Generator, Tuple, Dict

import logging
import zmq.green as zmq

from cxworker.docker import DockerContainer, DockerImage
from cxworker.docker.container import NvidiaDockerContainer
from .errors import ContainerError
from .config import ContainerConfig

WORKER_PROCESS_PORT = 9999


class Container:
    def __init__(self, socket: zmq.Socket, docker_container_class: type):
        self.socket = socket
        self.current_request = None
        self.master = None
        self.docker_container_class = docker_container_class
        self.docker_container: DockerContainer = None
        self.model_name = None
        self.model_version = None

    def set_model(self, name, version):
        self.model_name = name
        self.model_version = version


class ContainerRegistry:
    def __init__(self, zmq_context: zmq.Context, registry: str, container_config: Mapping[str, ContainerConfig],
                 autoremove_containers: bool):
        self.poller = zmq.Poller()
        self.containers: Dict[str, Container] = {}
        self.container_config = container_config
        self.registry = registry
        self.autoremove_containers = autoremove_containers

        for name, config in container_config.items():
            socket = zmq_context.socket(zmq.DEALER)

            if config.type == "cpu":
                container_class = DockerContainer
            elif config.type == "nvidia":
                container_class = NvidiaDockerContainer
            else:
                raise RuntimeError("Unknown container type: {}".format(config.type))

            self.containers[name] = Container(socket, container_class)
            self.poller.register(socket, zmq.POLLIN)

    def start_container(self, id: str, model: str, version: str, slaves: Tuple[str, ...] = ()):
        config = self.container_config[id]
        container = self.containers[id]

        for slave_id in slaves:
            slave_container = self.containers[slave_id]
            if slave_container.docker_container_class != container.docker_container_class:
                message = "The type of slave container {slave_id} ({slave_type}) " \
                          "is different from the type of the master ({master_type})"\
                    .format(slave_id=slave_id, slave_type=slave_container.docker_container_class,
                            master_type=container.docker_container_class)

                raise RuntimeError(message)

        if container.docker_container is not None:
            self.kill_container(id)

        image = DockerImage(model, tag=version, registry=self.registry)
        image.pull()

        container.docker_container = container.docker_container_class(self.registry, image.name,
                                                                      self.autoremove_containers)
        container.set_model(model, version)
        container.docker_container.add_port_mapping(config.port, WORKER_PROCESS_PORT)

        for slave_id in slaves:
            slave_config = self.container_config[slave_id]
            slave_container = self.containers[slave_id]
            if slave_container.docker_container is not None:
                self.kill_container(slave_id)

            slave_container.master = container

            for device in slave_config.devices:
                container.docker_container.add_device(device)

        container.docker_container.start()
        container.socket.connect("tcp://0.0.0.0:{}".format(config.port))

    def kill_container(self, id: str):
        container = self.containers[id]
        zmq_address = "tcp://0.0.0.0:{}".format(self.container_config[id].port)
        container.socket.disconnect(zmq_address)
        container.docker_container.kill()
        container.docker_container = None

    def send_input(self, container_id: str, request_metadata, input: bytes):
        container = self.containers[container_id]
        container.current_request = request_metadata
        container.socket.send_multipart([b"input", input])

    def wait_for_output(self) -> Generator[str, None, None]:
        """
        Wait until output arrives from one or more containers.
        :return: a generator of ids of containers from which we received output
        """

        result = self.poller.poll()

        return (id for id, container in self.containers.items() if (container.socket, zmq.POLLIN) in result)

    def read_output(self, container_id: str) -> str:
        message_type, message, *_ = self.containers[container_id].socket.recv_multipart()

        if message_type == "output":
            return message
        elif message_type == "error":
            raise ContainerError("The container encountered an error: " + message)
        else:
            raise ContainerError("The container responded with an unknown message type " + message_type)

    def get_status(self) -> Generator[dict, None, None]:
        """
        Get status information for all containers
        :return: a generator of status information
        """

        for name, container in self.containers.items():
            yield {
                "name": name,
                "running": container.docker_container is not None and container.docker_container.running,
                "request": container.current_request.id if container.current_request is not None else None,
                "model_name": container.model_name,
                "model_version": container.model_version
            }

    def kill_all(self):
        for name, container in self.containers.items():
            if container.docker_container is not None:
                self.kill_container(name)

    def get_current_request(self, container_id):
        return self.containers[container_id].current_request

    def request_finished(self, container_id):
        self.containers[container_id].current_request = None
