import pytest

from cxworker.sheep import BareSheep, DockerSheep, SheepConfigurationError

from ..docker.docker_not_available import docker_not_available


def test_bare_sheep_start_stop(bare_sheep: BareSheep):
    bare_sheep.slaughter()
    bare_sheep.start('cxflow-test', 'latest')
    assert bare_sheep.running
    bare_sheep.slaughter()
    assert not bare_sheep.running
    bare_sheep.start('cxflow-test', 'latest')


def test_bare_configuration_error(bare_sheep: BareSheep):

    with pytest.raises(SheepConfigurationError):  # path does not exist
        bare_sheep.start('cxflow-test', 'i-do-not-exist')


@pytest.mark.skipif(docker_not_available(), reason='Docker is not available.')
def test_docker_sheep_start_stop(docker_sheep: DockerSheep):
    docker_sheep.start('pritunl/archlinux', 'latest')
    assert docker_sheep.running
    docker_sheep.slaughter()
    assert not docker_sheep.running
    docker_sheep.start('base/archlinux', '')
    docker_sheep.start('base/archlinux', '')


@pytest.mark.skipif(docker_not_available(), reason='Docker is not available.')
def test_docker_configuration_error(docker_sheep: DockerSheep):
    with pytest.raises(SheepConfigurationError):  # image pull should fail
        docker_sheep.start('missing/image-sosjshd', 'latest')

    docker_sheep.sheep_data_root = 'i-do-not/exist'
    with pytest.raises(SheepConfigurationError):  # container start should fail
        docker_sheep.start('pritunl/archlinux', 'latest')
