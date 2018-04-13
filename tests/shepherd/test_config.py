import logging

import pytest
from schematics.exceptions import DataError

from cxworker.shepherd.config import load_worker_config, WorkerConfig


def test_load_config(valid_config_file):
    with open(valid_config_file) as file:
        config = load_worker_config(file)

    assert isinstance(config, WorkerConfig)

    assert config.data_root == '/tmp/worker-data'

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


def test_invalid_config(invalid_config_file):

    with pytest.raises(DataError):
        with open(invalid_config_file) as file:
            _ = load_worker_config(file)
