import logging

import click
import emloop as el
from minio import Minio

from aiohttp import web
import aiohttp_cors

from .storage import MinioStorage
from .api import create_app
from .shepherd import Shepherd
from .sheep.welcome import welcome
from .api.views import create_shepherd_routes
from .config import load_shepherd_config


@click.command()
@click.option("-h", "--host", default="0.0.0.0", help="The host name to which the HTTP API should bind")
@click.option("-p", "--port", default=5000, help="The port to which the HTTP API should bind")
@click.option("-c", "--config", "config_file", required=True, help="Path to a configuration file")
def run(host, port, config_file) -> None:
    """
    Run shepherd configured from the given ``config_file`` and listen for the API call on the given ``host`` and ``port``.

    :param host: API host
    :param port: API port
    :param config_file: shepherd config file
    """
    # load shepherd configuration
    with open(config_file, "r") as config_stream:
        config = load_shepherd_config(config_stream)

    # set-up logging
    logging.basicConfig(level=config.logging.log_level,
                        format=el.constants.EL_LOG_FORMAT,
                        datefmt=el.constants.EL_LOG_DATE_FORMAT)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    welcome()

    # create minio, shepherd and API handles
    logging.debug('Creating minio handle')
    minio = Minio(config.storage.schemeless_url, config.storage.access_key, config.storage.secret_key,
                  config.storage.secure)
    storage = MinioStorage(minio)

    logging.debug('Creating shepherd')
    shepherd = Shepherd(config.sheep, config.data_root, storage, config.registry)

    app = create_app()
    app.add_routes(create_shepherd_routes(shepherd, storage))

    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
        )
    })

    for route in app.router.routes():
        cors.add(route)

    app.on_startup.append(lambda _: shepherd.start())
    app.on_cleanup.append(lambda _: shepherd.close())

    # process API calls forever
    try:
        logging.info('Shepherd API is available at http://%s:%s', host, port)
        web.run_app(app, host=host, port=port)
    except KeyboardInterrupt:
        logging.info("Interrupt caught, slaughtering all the sheep")
        shepherd.close()


if __name__ == "__main__":
    run()  # pragma: no cover
