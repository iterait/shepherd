import click
import zmq


@click.command()
@click.argument("payload_file")
def cli(payload_file):
    with open(payload_file, "rb") as payload:
        context = zmq.Context()
        socket = context.socket(zmq.DEALER)
        socket.send_multipart([payload])
        response, *_ = socket.recv_multipart()
        print(response)


if __name__ == "__main__":
    cli()
