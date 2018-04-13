import pytest
import os.path as path

from cxworker.shepherd.config import load_worker_config


@pytest.fixture()
def valid_config_file():
    yield path.join('examples', 'configs', 'cxworker-bare.yml')


@pytest.fixture()
def invalid_config_file():
    yield path.join('examples', 'docker', 'docker-compose-sandbox.yml')


@pytest.fixture()
def valid_config(valid_config_file):
    with open(valid_config_file) as file:
        yield load_worker_config(file)
