import click
import zmq
import sys


@click.command()
@click.argument("container_ip")
@click.argument("payload_file")
def cli(container_ip, payload_file):
    with open(payload_file, "rb") as payload:
        context = zmq.Context()
        socket = context.socket(zmq.DEALER)
        socket.connect("tcp://" + container_ip)
        socket.send_multipart([b"input", payload.read()])
        message_type, response, *_ = socket.recv_multipart()

        if message_type == b"output":
            print(response)
        elif message_type == b"error":
            print(response, file=sys.stderr)
            sys.exit(1)
        else:
            print("Unknown message received - type: {}, content: {}".format(message_type, response), file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    cli()
