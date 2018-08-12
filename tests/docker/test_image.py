import pytest

from cxworker.shepherd.config import RegistryConfig
from cxworker.docker import DockerImage, DockerError

from .docker_not_available import docker_not_available


@pytest.mark.skipif(docker_not_available(), reason='Docker is not available.')
def test_docker_image(registry_config, image_valid):
    image = DockerImage(*image_valid, registry_config)
    assert image.full_name == f'registry.hub.docker.com/{image_valid[0]}:{image_valid[1]}'
    image.pull()


@pytest.mark.skipif(docker_not_available(), reason='Docker is not available.')
def test_bad_docker_image(registry_config, image_valid, image_invalid):
    bad_registry_config = RegistryConfig(dict(url='registry.hub.docker.com',
                                              username='fasdfsdf', password='abc321321'))  # bad username
    image = DockerImage(*image_valid, bad_registry_config)
    with pytest.raises(DockerError):
        image.pull()

    image = DockerImage(*image_invalid, registry_config)  # bad image name
    with pytest.raises(DockerError):
        image.pull()
