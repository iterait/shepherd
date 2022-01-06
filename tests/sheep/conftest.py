import pytest
import zmq
import zmq.asyncio
from pathlib import Path
from zmq.error import ZMQError

from shepherd.sheep import BareSheep, DockerSheep
from shepherd.config import RegistryConfig


@pytest.fixture()
async def sheep_socket(loop):
    sock = zmq.asyncio.Context.instance().socket(zmq.DEALER)
    yield sock
    try:
        sock.disconnect('tcp://0.0.0.0:9001')
    except ZMQError:  # the socket may have been disconnected
        pass
    sock.close()


@pytest.fixture()
def bare_sheep_config(tmpdir_factory):
    tmpdir = Path(tmpdir_factory.mktemp('logs'))
    yield {'port': 9001, 'type': 'bare', 'working_directory': 'examples/docker/emloop_example',
           'stdout_file': str(tmpdir / 'bare-shepherd-runner-stdout.txt'),
           'stderr_file': str(tmpdir / 'bare-shepherd-runner-stderr.txt')}

    
@pytest.fixture()
async def bare_sheep(sheep_socket, tmpdir, bare_sheep_config):
    sheep = BareSheep({'port': 9001, 'type': 'bare', 'working_directory': 'examples/docker/emloop_example',
                       'stdout_file': '/tmp/i-dont-exists/bare-shepherd-runner-stdout.txt',
                       'stderr_file': '/tmp/i-dont-exists/bare-shepherd-runner-stderr.txt'},
                      socket=sheep_socket, sheep_data_root=str(tmpdir), sheep_id='sheep')
    yield sheep
    sheep.slaughter()


@pytest.fixture()
async def docker_sheep(sheep_socket):
    registry_config = RegistryConfig(dict(url=''))
    sheep = DockerSheep({'port': 9001, 'type': 'docker'}, registry_config,
                        socket=sheep_socket, sheep_data_root='/tmp', command=['sleep', '2'], sheep_id='sheep')
    yield sheep
    sheep.slaughter()
