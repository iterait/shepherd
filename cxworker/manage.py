from gevent import monkey; monkey.patch_all(sys=True, Event=True)

import sys
import logging

import click
import gevent
import cxflow as cx
from minio import Minio
from gevent.wsgi import WSGIServer
from urllib3.exceptions import MaxRetryError

from .api import create_app
from .shepherd import Shepherd
from .sheep.welcome import welcome
from .api.views import create_worker_blueprint
from .shepherd.config import load_worker_config


@click.command()
@click.option("-h", "--host", default="0.0.0.0", help="The host name to which the HTTP API should bind")
@click.option("-p", "--port", default=5000, help="The port to which the HTTP API should bind")
@click.option("-c", "--config", "config_file", required=True, help="Path to a configuration file")
def run(host, port, config_file) -> None:
    """
    Run worker configured from the given ``config_file`` and listen for the API call on the given ``host`` and ``port``.

    :param host: API host
    :param port: API port
    :param config_file: worker config file
    """
    # load worker configuration
    with open(config_file, "r") as config_stream:
        config = load_worker_config(config_stream)

    # set-up logging
    logging.basicConfig(level=config.logging.log_level,
                        format=cx.constants.CXF_LOG_FORMAT,
                        datefmt=cx.constants.CXF_LOG_DATE_FORMAT)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    welcome()

    # create minio, shepherd and API handles
    logging.debug('Creating minio handle')
    minio = Minio(config.storage.schemeless_url, config.storage.access_key, config.storage.secret_key,
                  config.storage.secure)
    logging.debug('Creating shepherd')
    shepherd = Shepherd(config.sheep, config.data_root, minio, config.registry)

    app = create_app(__name__)
    app.register_blueprint(create_worker_blueprint(shepherd, minio))
    api_server = WSGIServer((host, port), app, log=logging.getLogger(''))
    api_handler = gevent.spawn(api_server.serve_forever)

    # everything should be ready, lets check if minio works
    try:
        minio.list_buckets()
        logging.info('Minio storage appears to be up and running')
    except MaxRetryError:
        logging.error('Cannot connect to minio at (%s)', config.storage.url)
        sys.exit(1)

    # process API calls forever
    try:
        logging.info('Worker API is available at http://%s:%s', host, port)
        api_handler.join()
    except KeyboardInterrupt:
        logging.info("Interrupt caught, slaughtering all the sheep")
        shepherd.close()


if __name__ == "__main__":
    run()  # pragma: no cover
