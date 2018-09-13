import pytest
import zmq.green as zmq
from zmq.error import ZMQError

from shepherd.sheep import BareSheep, DockerSheep, SheepError, SheepConfigurationError
from shepherd.shepherd.config import RegistryConfig


@pytest.fixture()
def sheep_socket():
    sock = zmq.Context.instance().socket(zmq.DEALER)
    yield sock
    try:
        sock.disconnect('tcp://0.0.0.0:9001')
    except ZMQError:  # the socket may have been disconnected
        pass
    sock.close()


@pytest.fixture()
def bare_sheep(sheep_socket, tmpdir):
    sheep = BareSheep({'port': 9001, 'type': 'bare', 'working_directory': 'examples/docker/cxflow_example',
                       'stdout_file': '/tmp/i-dont-exists/bare-shepherd-runner-stdout.txt',
                       'stderr_file': '/tmp/i-dont-exists/bare-shepherd-runner-stderr.txt'},
                      socket=sheep_socket, sheep_data_root=str(tmpdir))
    yield sheep
    sheep.slaughter()


@pytest.fixture()
def docker_sheep(sheep_socket):
    registry_config = RegistryConfig(dict(url=''))
    sheep = DockerSheep({'port': 9001, 'type': 'docker'}, registry_config,
                        socket=sheep_socket, sheep_data_root='/tmp', command=['sleep', '2'])
    yield sheep
    sheep.slaughter()
