import gevent
from gevent.wsgi import WSGIServer
import zmq.green as zmq

from .api.views import create_worker_blueprint
from .api import create_app
from .manager.registry import ContainerRegistry
from .manager.config import load_config
from .manager.output_listener import OutputListener


class Worker:
    def __init__(self):
        self.zmq_context = zmq.Context()
        self.app = create_app()
        self.registry = None
        self.config = None

    def load_config(self, config_stream):
        self.config = load_config(config_stream)
        self.registry = ContainerRegistry(self.zmq_context, self.config.registry, self.config.containers)

    def run(self, host: str, port: int):
        if self.config is None:
            raise RuntimeError("Configuration has not been loaded yet")

        self.app.register_blueprint(create_worker_blueprint(self.registry))

        api_server = WSGIServer((host, port), self.app)
        output_listener = OutputListener(self.registry)

        api = gevent.spawn(api_server.start)
        output = gevent.spawn(output_listener.listen)

        try:
            gevent.joinall([api, output])
        except KeyboardInterrupt:
            self.registry.kill_all()
