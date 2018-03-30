import logging
import json
import os.path as path
from typing import Dict, Any

import gevent
import zmq.green as zmq
from zmq import Socket

from .bare_sheep import BaseSheep


__all__ = ['DummySheep']


class DummySheep(BaseSheep):
    running = False

    def __init__(self, config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = self.Config(config)
        self.server = None
        self.feeding_socket: Socket = None

    def update_model(self):
        pass

    def start(self, model_name: str, model_version: str):
        super().start(model_name, model_version)
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
        super().slaughter()
        if self.server is not None:
            self.server.kill()
            self.server = None
        if self.feeding_socket is not None:
            self.feeding_socket.close(0)
            self.feeding_socket = None
        self.running = False
