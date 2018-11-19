from unittest import mock
import pytest

from shepherd.api import create_app
from shepherd.api.models import SheepModel
from shepherd.api.views import create_shepherd_routes
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

    def ready(*args):
        return args[0] == 'uuid-ready'

    m = mock.create_autospec(Shepherd)
    m.get_status.side_effect = status_gen
    m.is_job_done.side_effect = ready
    m.notifier = mock.MagicMock()
    yield m


@pytest.fixture(scope="function")
def app(minio, mock_shepherd):
    app = create_app(__name__)
    app.register_blueprint(create_shepherd_routes(mock_shepherd, minio))
    app.debug = True

    with app.app_context():
        yield app
