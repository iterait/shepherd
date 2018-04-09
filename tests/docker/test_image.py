import pytest

from cxworker.shepherd.config import RegistryConfig
from cxworker.docker import DockerImage, DockerError

from .docker_not_available import docker_not_available


@pytest.mark.skipif(docker_not_available(), reason='Docker is not available.')
def test_docker_image(registry_config):
    image = DockerImage('pritunl/archlinux', 'latest', registry_config)
    assert image.full_name == 'registry.hub.docker.com/pritunl/archlinux:latest'
    image.pull()


@pytest.mark.skipif(docker_not_available(), reason='Docker is not available.')
def test_bad_docker_image(registry_config):
    bad_registry_config = RegistryConfig(dict(url='registry.hub.docker.com',
                                              username='fasdfsdf', password='abc321321'))  # bad username
    image = DockerImage('pritunl/archlinux', 'latest', bad_registry_config)
    with pytest.raises(DockerError):
        image.pull()

    image = DockerImage('pritunl/archlinudfgdfgx', 'latest', registry_config)  # bad image name
    with pytest.raises(DockerError):
        image.pull()
