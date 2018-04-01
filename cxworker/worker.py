import sys
import logging
from typing import Optional

import gevent
import cxflow as cx
from minio import Minio
from gevent.wsgi import WSGIServer
from urllib3.exceptions import MaxRetryError

from .shepherd import Shepherd
from .api import create_app
from .api.views import create_worker_blueprint
from .shepherd.config import WorkerConfig
from .sheep.welcome import welcome


__all__ = ['Worker']


class Worker:
    """
    Create and manage worker API, shepherd and minio handle.
    """

    def __init__(self):
        """Create new Worker."""
        self._minio: Optional[Minio] = None
        self._shepherd: Optional[Shepherd] = None
        self._config: Optional[WorkerConfig] = None
        self._app = create_app(__name__)

    @property
    def app(self):
        """Flask app serving the worker API."""
        return self._app

    def load_config(self, config: WorkerConfig) -> None:
        """
        Load the given configuration and create minio and shepherd handles.

        :param config: worker configuration to be loaded
        """
        self._config = config
        logging.basicConfig(level=self._config.logging.log_level,
                            format=cx.constants.CXF_LOG_FORMAT,
                            datefmt=cx.constants.CXF_LOG_DATE_FORMAT)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        welcome()

        logging.debug('Creating minio handle')
        self._minio = Minio(self._config.storage.schemeless_url, self._config.storage.access_key,
                            self._config.storage.secret_key, self._config.storage.secure)

        logging.debug('Creating shepherd')
        self._shepherd = Shepherd(self._config.registry, self._config.sheep, self._config.data_root, self._minio)

    def run(self, host: str, port: int) -> None:
        """
        Run the API server, and shepherd.
        The worker has to be previously configured with :py:meth:`load_config`.

        :param host: API host
        :param port: API port
        """
        if self._config is None:
            raise RuntimeError("Configuration has not been loaded yet")

        self._app.register_blueprint(create_worker_blueprint(self._shepherd, self._minio))

        api_server = WSGIServer((host, port), self._app, log=logging.getLogger(''))

        api_handler = gevent.spawn(api_server.start)
        sheep_listener = gevent.spawn(self._shepherd.listen)

        # everything should be ready, lets check if minio works
        try:
            self._minio.list_buckets()
            logging.info('Minio storage appears to be up and running')
        except MaxRetryError:
            logging.error('Cannot connect to minio at (%s)',self._config.storage.url)
            sys.exit(1)

        try:
            logging.info('Worker API is available at http://%s:%s', host, port)
            gevent.joinall([api_handler, sheep_listener])
        except KeyboardInterrupt:
            logging.info("Interrupt caught, slaughtering all the sheep")
            self._shepherd.slaughter_all()
