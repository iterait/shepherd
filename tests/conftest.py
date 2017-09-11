import pytest
from unittest import mock

import zmq
from minio import Minio

from cxworker.api import create_app
from cxworker.api.views import create_worker_blueprint
from cxworker.manager.config import ContainerConfig
from cxworker.manager.registry import ContainerRegistry


@pytest.fixture()
def app(minio, registry):
    app = create_app(__name__)
    app.register_blueprint(create_worker_blueprint(registry, minio))

    with app.app_context():
        yield app


@pytest.fixture()
def minio():
    yield mock.Mock(spec=Minio)


@pytest.fixture()
def registry(zmq_context, container_config):
    yield ContainerRegistry(zmq_context, "fake_registry", container_config)


@pytest.fixture()
def zmq_context():
    yield mock.Mock(spec=zmq.Context)


@pytest.fixture()
def container_config():
    yield {
        "container_a": ContainerConfig(8888, "cpu", []),
        "container_b": ContainerConfig(8889, "cpu", [])
    }
