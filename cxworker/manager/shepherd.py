import logging
from zmq.error import ZMQError
import zmq.green as zmq
from typing import Mapping, Generator, Tuple, Dict, Any, Optional

from cxworker.api.models import SheepModel
from cxworker.sheep.adapters import DockerSheep, BareSheep, SheepAdapter
from cxworker.errors import SheepConfigurationError
from cxworker.sheep.testing import DummySheep
from .errors import SheepError
from ..api.errors import UnknownSheepError
from .config import RegistryConfig


class RuntimeSheepData:
    """
    A holder for information about a sheep that are not dependent on its exact implementation (e.g. Docker).
    """

    def __init__(self, socket: zmq.Socket, sheep_type, adapter: SheepAdapter):
        self.socket = socket
        self.current_request = None
        self.model_name = None
        self.model_version = None
        self.sheep_type = sheep_type
        self.adapter = adapter
        self.bellwether = None
        """
        A sheep that leads the herd (often wearing a bell).
        """

    def set_model(self, name, version):
        self.model_name = name
        self.model_version = version


class Shepherd:
    """
    Manages creation and access to a configured set of sheep
    """

    def __init__(self, zmq_context: zmq.Context, registry: Optional[RegistryConfig],
                 sheep_config: Mapping[str, Dict[str, Any]]):
        self.poller = zmq.Poller()
        self.sheep: Dict[str, RuntimeSheepData] = {}

        for sheep_id, config in sheep_config.items():
            socket = zmq_context.socket(zmq.DEALER)
            sheep_type = config.get("type", None)

            if sheep_type is None:
                raise SheepConfigurationError("No type specified for sheep '{}'".format(sheep_id))

            if sheep_type == "docker":
                if registry is None:
                    raise SheepConfigurationError("To use docker sheep, you need to configure a registry URL")
                adapter = DockerSheep(config, registry)
            elif sheep_type == "bare":
                adapter = BareSheep(config)
            elif sheep_type == "dummy":
                adapter = DummySheep(config)
            else:
                raise SheepConfigurationError("Unknown sheep type: {}".format(sheep_type))

            logging.info('Creating sheep `%s` of type `%s`', sheep_id, sheep_type)
            self.sheep[sheep_id] = RuntimeSheepData(socket, sheep_type, adapter)
            self.poller.register(socket, zmq.POLLIN)

    def __getitem__(self, sheep_id: str) -> RuntimeSheepData:
        """
        Get the sheep with the given `sheep_id`.

        :param sheep_id: sheep id
        :return: sheep with the given ``sheep_id``
        :raise UnknownContainerError: if the given ``sheep_id`` is not known to this registry
        """
        if sheep_id not in self.sheep:
            raise UnknownSheepError('Unknown sheep id `{}`'.format(sheep_id))
        return self.sheep[sheep_id]

    def start_sheep(self, sheep_id: str, model: str, version: str, herd_members: Tuple[str, ...] = ()):
        logging.info('Starting sheep `%s` with model `%s:%s`', sheep_id, model, version)
        sheep = self[sheep_id]

        for herd_member_id in herd_members:
            herd_member = self[herd_member_id]
            if herd_member.sheep_type != sheep.sheep_type:
                message = "The type of slave sheep {slave_id} ({slave_type}) " \
                          "is different from the type of the master ({master_type})"\
                    .format(slave_id=herd_member_id, slave_type=herd_member.sheep_type,
                            master_type=sheep.sheep_type)

                raise RuntimeError(message)

        if sheep.adapter.running:
            self.slaughter_sheep(sheep_id)

        sheep.adapter.load_model(model, version)
        sheep.set_model(model, version)

        for herd_member_id in herd_members:
            herd_member = self[herd_member_id]
            if herd_member.adapter.running:
                self.slaughter_sheep(herd_member_id)

            herd_member.bellwether = sheep

        sheep.adapter.start(tuple(map(lambda member_id: self[member_id].adapter, herd_members)))
        sheep.socket.connect("tcp://0.0.0.0:{}".format(sheep.adapter.config.port))

    def refresh_model(self, sheep_id: str):
        sheep = self[sheep_id]

        # If there is an update, restart the sheep
        if sheep.adapter.update_model():
            current_herd_members = tuple(sheep_id for sheep_id, sheep in self.sheep.items()
                                         if sheep.bellwether == sheep)
            self.start_sheep(sheep_id, sheep.model_name, sheep.model_version, current_herd_members)

    def slaughter_sheep(self, sheep_id: str):
        logging.info('Slaughtering sheep `%s`', sheep_id)
        sheep = self[sheep_id]
        zmq_address = "tcp://0.0.0.0:{}".format(sheep.adapter.config.port)
        try:
            sheep.socket.disconnect(zmq_address)
        except ZMQError:
            logging.warning('Failed to disconnect socket of `{}`  (perhaps it was not started/connected)'
                            .format(sheep_id))
        sheep.adapter.slaughter()

    def send_input(self, sheep_id: str, request_metadata, input: bytes):
        sheep = self[sheep_id]
        sheep.current_request = request_metadata
        sheep.socket.send_multipart([b"input", input])

    def wait_for_output(self) -> Generator[str, None, None]:
        """
        Wait until output arrives from one or more sheep.
        :return: a generator of ids of sheep from which we received output
        """

        result = self.poller.poll()
        return (sheep_id for sheep_id, sheep in self.sheep.items()
                if (sheep.socket, zmq.POLLIN) in result)

    def read_output(self, sheep_id: str) -> str:
        message_type, message, *rest = self[sheep_id].socket.recv_multipart()

        if message_type == b"output":
            return message
        elif message_type == b"error":
            if len(rest) >= 1:
                logging.error("Received error traceback:")
                logging.error(rest[0])
            raise SheepError("The sheep encountered an error: " + message.decode())
        else:
            raise SheepError("The sheep responded with an unknown message type " + message_type.decode())

    def get_status(self) -> Generator[Tuple[str, SheepModel], None, None]:
        """
        Get status information for all sheep
        :return: a generator of status information
        """

        for sheep_id, sheep in self.sheep.items():
            yield sheep_id, SheepModel({
                "running": sheep.adapter.running,
                "request": sheep.current_request.id if sheep.current_request is not None else None,
                "model": {
                    "name": sheep.model_name,
                    "version": sheep.model_version
                }
            })

    def slaughter_all(self):
        for sheep_id, sheep in self.sheep.items():
            if sheep.adapter.running is not None:
                self.slaughter_sheep(sheep_id)

    def get_current_request(self, sheep_id):
        return self[sheep_id].current_request

    def request_finished(self, sheep_id):
        self[sheep_id].current_request = None
