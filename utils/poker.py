import click
import zmq


@click.command()
@click.argument("container_ip")
@click.argument("payload_file")
def cli(container_ip, payload_file):
    with open(payload_file, "rb") as payload:
        context = zmq.Context()
        socket = context.socket(zmq.DEALER)
        socket.connect("tcp://" + container_ip)
        socket.send_multipart([payload.read()])
        response, *_ = socket.recv_multipart()
        print(response)


if __name__ == "__main__":
    cli()
