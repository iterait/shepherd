from typing import Mapping, Generator
import zmq.green as zmq

from CXWorker.docker import DockerContainer, DockerImage

WORKER_PROCESS_PORT = 9999


class Container:
    def __init__(self, socket: zmq.Socket):
        self.socket = socket
        self.current_request = None
        self.docker_container: DockerContainer = None


class ContainerConfig:
    def __init__(self, port: int):
        self.port = port


class ContainerRegistry:
    def __init__(self):
        self.poller = zmq.Poller()
        self.containers = {}
        self.container_config = {}
        self.initialized = False
        self.registry = ""

    def check_initialized(self):
        if not self.initialized:
            raise RuntimeError("The registry was not initialized yet")

    def initialize(self, zmq_context: zmq.Context, registry: str, container_config: Mapping[str, ContainerConfig]):
        self.container_config = container_config
        self.registry = registry

        for name, config in container_config.items():
            socket = zmq_context.socket(zmq.ROUTER)

            self.containers[name] = Container(socket)
            self.poller.register(socket, zmq.POLLIN)

        self.initialized = True

    def start_container(self, id: str, model: str, version: str):
        self.check_initialized()

        container = self.containers[id]
        config = self.container_config[id]
        zmq_address = "tcp://0.0.0.0:{}".format(config.port)

        if container.docker_container is not None:
            self.kill_container(id)

        image = DockerImage(model, tag=version, registry=self.registry)
        image.pull()

        container.docker_container = DockerContainer(image.name)
        container.docker_container.add_port_mapping(config.port, WORKER_PROCESS_PORT)
        container.docker_container.start()

        container.socket.connect(zmq_address)

    def kill_container(self, id: str):
        self.check_initialized()

        container = self.containers[id]
        zmq_address = "tcp://0.0.0.0:{}".format(self.container_config[id].port)
        container.socket.disconnect(zmq_address)
        container.docker_container.kill()
        container.docker_container = None

    def send_input(self, container_id: str, input: bytes):
        self.check_initialized()
        self.containers[container_id].socket.send_multipart([b"container", input])

    def wait_for_output(self) -> Generator[str, None, None]:
        """
        Wait until output arrives from one or more containers.
        :return: a generator of ids of containers from which we received output
        """

        self.check_initialized()
        result = self.poller.poll()

        return (id for id, container in self.containers.items() if (container.socket, zmq.POLLIN) in result)

    def read_output(self, container_id: str) -> str:
        self.check_initialized()
        identity, message = self.containers[container_id].socket.recv_multipart()
        return message

    def get_status(self) -> Generator[dict, None, None]:
        """
        Get status information for all containers
        :return: a generator of status information
        """

        self.check_initialized()

        for name, container in self.containers.items():
            yield {
                "name": name,
                "status": container.docker_container is not None and container.docker_container.running,
                "request": container.current_request,
                "model_name": "",
                "model_version": ""
            }

    def kill_all(self):
        for name, container in self.containers.items():
            if container.docker_container is not None:
                self.kill_container(name)
