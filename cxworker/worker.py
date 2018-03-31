import sys
import logging

import gevent
import zmq.green as zmq
from minio import Minio
from gevent.wsgi import WSGIServer
from urllib3.exceptions import MaxRetryError

from .shepherd import Shepherd
from .api.views import create_worker_blueprint
from .api import create_app
from .shepherd.config import load_config, WorkerConfig


class Worker:
    def __init__(self):
        self.zmq_context = zmq.Context.instance()
        self.app = create_app(__name__)
        self.shepherd: Shepherd = None
        self.minio = None
        self.config: WorkerConfig = None

    def load_config(self, config_stream):
        self.config = load_config(config_stream)
        logging.basicConfig(level=self.config.logging.log_level)
        logging.getLogger("urllib3").setLevel(logging.WARNING)

        logging.debug('Creating minio handle')
        self.minio = Minio(self.config.storage.schemeless_url, self.config.storage.access_key,
                           self.config.storage.secret_key, self.config.storage.secure)

        logging.debug('Creating shepherd')
        self.shepherd = Shepherd(self.zmq_context, self.config.registry, self.config.containers, self.config.data_root,
                                 self.minio)

    def run(self, host: str, port: int):
        if self.config is None:
            raise RuntimeError("Configuration has not been loaded yet")

        self.app.register_blueprint(create_worker_blueprint(self.shepherd, self.minio))

        api_server = WSGIServer((host, port), self.app)

        api_handler = gevent.spawn(api_server.start)
        sheep_listener = gevent.spawn(self.shepherd.listen)

        # everything should be ready, lets check if minio works
        try:
            self.minio.list_buckets()
            logging.info('Minio storage appears to be up and running')
        except MaxRetryError:
            logging.error('Could not list minio buckets. Verify minio url ({}) and credentials'
                          .format(self.config.storage.url))
            sys.exit(1)

        try:
            logging.info('Listening for API calls at http://%s:%s (send a keyboard interrupt to stop the worker)',
                         host, port)
            gevent.joinall([api_handler, sheep_listener])
        except KeyboardInterrupt:
            logging.info("Interrupt caught, slaughtering all the sheep")
            self.shepherd.slaughter_all()

