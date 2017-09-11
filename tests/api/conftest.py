import pytest


@pytest.fixture()
def api_client(app):
    with app.test_client() as client:
        yield client
