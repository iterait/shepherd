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
        socket.bind(f"tcp://*:{self.config.port}")

        while True:
            identity, message_type, *_ = socket.recv_multipart()

            if message_type == b"input":
                gevent.sleep(5)
                socket.send_multipart([identity, b"output", b"{}"])

    def kill(self):
        self.server.kill()
        self.running = False
