import subprocess
import gevent

import pytest

from shepherd.docker import DockerError
from shepherd.docker.utils import run_docker_command, kill_blocking_container

from .docker_not_available import docker_not_available


@pytest.mark.skipif(docker_not_available(), reason='Docker is not available.')
def test_run_command(caplog):

    out = run_docker_command(['ps'])
    assert out.startswith('CONTAINER ID')

    with pytest.raises(DockerError):
        run_docker_command(['kill', 'idonotexist'])
    assert 'Error' in caplog.text


@pytest.mark.skipif(docker_not_available(), reason='Docker is not available.')
def test_kill_blocking_container(image_valid):
    # warm-up (pulling the image)
    proc = subprocess.Popen(['docker', 'run', '--rm', '-p' '9999:9999', image_valid[0], 'echo', 'hello'])
    proc.wait()

    proc = subprocess.Popen(['docker', 'run', '--rm', '-p' '9999:9999', image_valid[0], 'sleep', '10'])
    for _ in range(20):
        gevent.sleep(0.2)
        if len(run_docker_command(['ps']).split('\n')) > 2:
            break
    else:
        assert False
    assert proc.poll() is None
    kill_blocking_container(9999)
    for _ in range(20):
        gevent.sleep(0.2)
        if proc.poll() is not None:
            break
    else:
        assert False
