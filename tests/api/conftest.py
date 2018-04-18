from unittest import mock

import pytest

from cxworker.api import create_app
from cxworker.api.views import create_worker_blueprint
from cxworker.shepherd import Shepherd


@pytest.fixture(scope="function")
def mock_shepherd():
    yield mock.create_autospec(Shepherd)


@pytest.fixture(scope="function")
def app(minio, mock_shepherd):
    app = create_app(__name__)
    app.register_blueprint(create_worker_blueprint(mock_shepherd, minio))

    with app.app_context():
        yield app
