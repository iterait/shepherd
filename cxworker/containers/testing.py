import logging
import gevent
import zmq.green as zmq
from typing import Dict, Any, Sequence

from cxworker.containers.adapters import ContainerAdapter


class DummyContainerAdapter(ContainerAdapter):
    running = False

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.config = self.Config(**config)
        self.server = None

    def load_model(self, model_name: str, model_version: str):
        pass

    def update_model(self):
        pass

    def start(self, slaves: Sequence[ContainerAdapter]):
        self.running = True
        self.server = gevent.spawn(self.serve)

    def serve(self):
        context = zmq.Context()
        socket = context.socket(zmq.ROUTER)
        address = f"tcp://*:{self.config.port}"
        socket.bind(address)

        logging.info('Testing container is listening at `%s`', address)
        while True:
            identity, message_type, *_ = socket.recv_multipart()
            logging.debug('Received message `%s` of type `%s`', identity, message_type)

            if message_type == b"input":
                gevent.sleep(5)
                object_name, body = b"output", b"{}"
                logging.debug('Sending response `%s` to `%s`', body, object_name)
                socket.send_multipart([identity, object_name, body])

    def kill(self):
        if self.server is not None:
            self.server.kill()
        self.running = False
