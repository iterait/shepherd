import subprocess
import gevent

import pytest

from cxworker.docker import DockerError
from cxworker.docker.utils import run_docker_command, kill_blocking_container


def test_run_command(caplog):

    out = run_docker_command(['ps'])
    assert out.startswith('CONTAINER ID')

    with pytest.raises(DockerError):
        run_docker_command(['kill', 'idonotexist'])
    assert 'Error' in caplog.text


def test_kill_blocking_container():
    # warm-up (pulling the image)
    proc = subprocess.Popen(['docker', 'run', '--rm', '-p' '9999:9999', 'pritunl/archlinux', 'echo', 'hello'])
    proc.wait()

    proc = subprocess.Popen(['docker', 'run', '--rm', '-p' '9999:9999', 'pritunl/archlinux', 'sleep', '10'])
    gevent.sleep(2)  # allow docker start-up
    assert proc.poll() is None
    kill_blocking_container(9999)
    gevent.sleep(0.2)
    assert proc.poll() is not None
