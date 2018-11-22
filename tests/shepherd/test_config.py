import logging

import pytest
import os
from schematics.exceptions import DataError

from shepherd.config import load_shepherd_config, ShepherdConfig


def test_load_config(valid_config_file):
    with open(valid_config_file) as file:
        config = load_shepherd_config(file)

    assert isinstance(config, ShepherdConfig)

    assert config.data_root == '/tmp/shepherd-data'

    assert config.storage.url == 'http://0.0.0.0:7000'
    assert config.storage.schemeless_url == '0.0.0.0:7000'
    assert not config.storage.secure
    assert config.storage.access_key == 'AKIAIOSFODNN7EXAMPLE'
    assert config.storage.secret_key == 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'

    assert config.logging.level == 'info'
    assert config.logging.log_level == logging.INFO

    assert config.registry.url == 'http://0.0.0.0:6000'

    assert config.sheep['bare_sheep']['type'] == 'bare'
    assert config.sheep['bare_sheep']['port'] == 9001


def test_load_config_valid_env(valid_config_env_file):
    os.environ['REGISTRY_URL'] = 'http://0.0.0.0:6000'
    os.environ['STORAGE_URL'] = 'http://0.0.0.0:7000'
    os.environ['STORAGE_ACCESS_KEY'] = 'AKIAIOSFODNN7EXAMPLE'
    os.environ['_STORAGE_SECRET_KEY'] = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    os.environ['SHEEP_PORT_1'] = '9001'
    os.environ['HOME_PATH'] = 'examples'
    os.environ['MODEL_EXAMPLE'] = 'emloop_example'

    with open(valid_config_env_file) as file:
        config = load_shepherd_config(file)

    assert isinstance(config, ShepherdConfig)

    assert config.data_root == '/tmp/shepherd-data'

    assert config.registry.url == os.environ['REGISTRY_URL']
    assert config.storage.url == os.environ['STORAGE_URL']
    assert config.storage.access_key == os.environ['STORAGE_ACCESS_KEY']
    assert config.storage.secret_key == os.environ['_STORAGE_SECRET_KEY']

    assert config.sheep['bare_sheep']['type'] == 'bare'
    assert config.sheep['bare_sheep']['port'] == os.environ['SHEEP_PORT_1']
    assert config.sheep['bare_sheep']['working_directory'] == os.path.join('filip',
                                                                           os.environ['HOME_PATH'],
                                                                           'docker',
                                                                           os.environ['MODEL_EXAMPLE'])


def test_load_config_invalid_env(valid_config_env_file):
    if 'REGISTRY_URL' in os.environ:
        del os.environ['REGISTRY_URL']

    os.environ['STORAGE_URL'] = 'http://0.0.0.0:7000'
    os.environ['STORAGE_ACCESS_KEY'] = 'AKIAIOSFODNN7EXAMPLE'
    os.environ['_STORAGE_SECRET_KEY'] = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    os.environ['SHEEP_PORT_1'] = '9001'
    os.environ['HOME_PATH'] = 'examples'
    os.environ['MODEL_EXAMPLE'] = 'emloop_example'

    with pytest.raises(ValueError, match='Environment variable `REGISTRY_URL` not set'), \
         open(valid_config_env_file) as file:
        load_shepherd_config(file)


def test_load_config_invalid_env_name(invalid_config_env_file):
    os.environ['REGISTRY_URL'] = 'http://0.0.0.0:6000'
    os.environ['STORAGE_URL'] = 'http://0.0.0.0:7000'
    os.environ['STORAGE_ACCESS_KEY'] = 'AKIAIOSFODNN7EXAMPLE'
    os.environ['_STORAGE_SECRET_KEY'] = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    os.environ['SHEEP_PORT_1'] = '9001'
    os.environ['HOME_PATH'] = 'examples'
    os.environ['MODEL_EXAMPLE'] = 'emloop_example'

    with open(invalid_config_env_file) as file:
        config = load_shepherd_config(file)

        assert isinstance(config, ShepherdConfig)
        assert config.sheep['bare_sheep']['name'] == '${3_SHEEP_NAME}'


def test_invalid_config(invalid_config_file):
    with pytest.raises(DataError), open(invalid_config_file) as file:
        load_shepherd_config(file)
