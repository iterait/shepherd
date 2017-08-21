import gevent
from gevent.wsgi import WSGIServer
import zmq.green as zmq

from .api import create_app
from .manager.registry import ContainerRegistry
from .manager.config import ContainerConfig, load_config
from .manager.output_listener import OutputListener


class Worker:
    def __init__(self):
        self.zmq_context = zmq.Context()
        self.registry = ContainerRegistry()
        self.app = create_app(self.registry)
        self.config = None

    def load_config(self, config_stream):
        self.config = load_config(config_stream)
        self.registry.initialize(self.zmq_context, self.config.registry, self.config.containers)

    def run(self, host: str, port: int):
        if self.config is None:
            raise RuntimeError("Configuration has not been loaded yet")

        api_server = WSGIServer((host, port), self.app)
        output_listener = OutputListener(self.registry)

        api = gevent.spawn(api_server.start)
        output = gevent.spawn(output_listener.listen)

        try:
            gevent.joinall([api, output])
        except KeyboardInterrupt:
            self.registry.kill_all()
