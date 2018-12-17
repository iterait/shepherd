from shepherd.docker.utils import run_docker_command
from shepherd.errors.docker import DockerError


def docker_not_available():
    try:
        run_docker_command(['version'])
    except DockerError:
        return True
    return False
