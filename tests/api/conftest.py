from unittest import mock
import pytest

from shepherd.api import create_app
from shepherd.api.models import SheepModel
from shepherd.api.views import create_shepherd_blueprint
from shepherd.shepherd import Shepherd


@pytest.fixture(scope="function")
def mock_shepherd():
    def status_gen():
        yield "bare_sheep", SheepModel({
                "running": False,
                "model": {
                    "name": "model_1",
                    "version": "latest"
                }
            })
    m = mock.create_autospec(Shepherd)
    m.get_status.side_effect = status_gen
    m.is_job_done.return_value = True
    m.notifier = mock.MagicMock()
    yield m


@pytest.fixture(scope="function")
def app(minio, mock_shepherd):
    app = create_app(__name__)
    app.register_blueprint(create_shepherd_blueprint(mock_shepherd, minio))

    with app.app_context():
        yield app
