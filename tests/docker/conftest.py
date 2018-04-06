import pytest

from cxworker.shepherd.config import RegistryConfig


@pytest.fixture()
def registry_config():
    yield RegistryConfig(dict(url='https://registry.hub.docker.com', username='cxworkertestdocker',
                              password='abc321321'))
