from gevent import monkey; monkey.patch_all(sys=True, Event=True)
import click
from flask.cli import shell_command

from cxworker.worker import Worker
from cxworker.shepherd.config import load_worker_config

worker = Worker()


@click.group()
@click.pass_context
def cli(ctx):
    ctx.obj = worker.app


@click.command()
@click.option("-h", "--host", default="", help="The host name to which the HTTP API should bind")
@click.option("-p", "--port", default=5000, help="The port to which the HTTP API should bind")
@click.option("-c", "--config", "config_file", default="cxworker.yml", help="Path to a configuration file")
def run_worker(host, port, config_file) -> None:
    """
    Run worker configured from the given ``config_file`` and listen for the API call on the given ``host`` and ``port``.

    :param host: API host
    :param port: API port
    :param config_file: worker config file
    """
    with open(config_file, "r") as config_stream:
        config = load_worker_config(config_stream)

    worker.load_config(config)
    worker.run(host, port)


cli.add_command(shell_command)
cli.add_command(run_worker)

if __name__ == "__main__":
    cli()
