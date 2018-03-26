import logging
import gevent
import zmq.green as zmq
from typing import Dict, Any, Sequence

from cxworker.sheep.adapters import SheepAdapter


class DummySheep(SheepAdapter):
    running = False

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.config = self.Config(config)
        self.server = None

    def load_model(self, model_name: str, model_version: str):
        pass

    def update_model(self):
        pass

    def start(self, herd_members: Sequence[SheepAdapter]):
        self.running = True
        self.server = gevent.spawn(self.serve)

    def serve(self):
        context = zmq.Context()
        socket = context.socket(zmq.ROUTER)
        address = f"tcp://*:{self.config.port}"
        socket.bind(address)

        logging.info('Testing container is listening at `%s`', address)
        while True:
            identity, message_type, payload, *_ = socket.recv_multipart()
            payload = payload.decode()
            logging.debug('Received message `%s` of type `%s`', payload, message_type)

            if message_type == b"input":
                gevent.sleep(3)
                body = bytes("I have seen it all: {}".format(payload), encoding='utf-8')
                logging.debug('Sending response `%s`', body)
                socket.send_multipart([identity, b"output", body])
            else:
                logging.debug('Sending error `%s`')
                socket.send_multipart([identity, b"error", b"Unrecognized message type"])

    def slaughter(self):
        if self.server is not None:
            self.server.slaughter()
        self.running = False
