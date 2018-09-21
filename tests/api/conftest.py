from unittest import mock

import pytest

from shepherd.api import create_app
from shepherd.api.views import create_shepherd_blueprint
from shepherd.shepherd import Shepherd


@pytest.fixture(scope="function")
def mock_shepherd():
    yield mock.create_autospec(Shepherd)


@pytest.fixture(scope="function")
def app(minio, mock_shepherd):
    app = create_app(__name__)
    app.register_blueprint(create_shepherd_blueprint(mock_shepherd, minio))
    app.debug = True

    with app.app_context():
        yield app
