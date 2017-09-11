import click
from flask.cli import shell_command

from cxworker.worker import Worker

worker = Worker()


@click.group()
@click.pass_context
def cli(ctx):
    ctx.obj = worker.app


@click.command()
@click.option("-h", "--host", default="", help="The host name to which the HTTP API should bind")
@click.option("-p", "--port", default=5000, help="The port to which the HTTP API should bind")
@click.option("-c", "--config", "config_file", default="cxworker.yml", help="Path to a configuration file")
def run_worker(host, port, config_file):
    with open(config_file, "r") as config:
        worker.load_config(config)

    worker.run(host, port)


cli.add_command(shell_command)
cli.add_command(run_worker)

if __name__ == "__main__":
    cli()
