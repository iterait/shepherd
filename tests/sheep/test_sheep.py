import pytest
import logging
from typing import Tuple

from shepherd.sheep import BareSheep, DockerSheep, SheepConfigurationError
from shepherd.sheep.docker_sheep import extract_gpu_number
from shepherd.sheep.welcome import welcome

from ..docker.docker_not_available import docker_not_available


def test_extract_gpu_number():
    assert extract_gpu_number('/dev/null') is None
    assert extract_gpu_number('/dev/nvidiactl') is None
    assert extract_gpu_number('/dev/nvidia1') == '1'
    assert extract_gpu_number('/dev/nvidia3') == '3'


def test_bare_sheep_start_stop(bare_sheep: BareSheep):
    bare_sheep.slaughter()
    bare_sheep.start('emloop-test', 'latest')
    assert bare_sheep.running
    bare_sheep.slaughter()
    assert not bare_sheep.running
    bare_sheep.start('emloop-test', 'latest')


def test_bare_configuration_error(bare_sheep: BareSheep):

    with pytest.raises(SheepConfigurationError):  # path does not exist
        bare_sheep.start('emloop-test', 'i-do-not-exist')


@pytest.fixture()
def image_valid2() -> Tuple[str, str]:
    yield 'library/alpine', 'edge'


@pytest.mark.skipif(docker_not_available(), reason='Docker is not available.')
def test_docker_sheep_start_stop(docker_sheep: DockerSheep, image_valid, image_valid2):
    docker_sheep.start(*image_valid)
    assert docker_sheep.running
    docker_sheep.slaughter()
    assert not docker_sheep.running
    docker_sheep.start(*image_valid2)
    docker_sheep.start(*image_valid2)


@pytest.mark.skipif(docker_not_available(), reason='Docker is not available.')
def test_docker_configuration_error(docker_sheep: DockerSheep, image_valid, image_invalid):
    with pytest.raises(SheepConfigurationError):  # image pull should fail
        docker_sheep.start(*image_invalid)

    docker_sheep.sheep_data_root = 'i-do-not/exist'
    with pytest.raises(SheepConfigurationError):  # container start should fail
        docker_sheep.start(*image_valid)


def test_welcome(caplog):
    caplog.set_level(logging.INFO)
    welcome()
    assert len(caplog.text) > 0
