import json
from flask.testing import Client


def test_status_basic(api_client: Client):
    response = api_client.get("/status", content_type="application/json")
    assert response.status_code == 200

    data = json.loads(response.data.decode())
    assert isinstance(data, dict)

    assert data == {
        "container_a": {
            "model": {
                "name": None,
                "version": None
            },
            "request": None,
            "running": False
        },
        "container_b": {
            "model": {
                "name": None,
                "version": None
            },
            "request": None,
            "running": False
        }
    }

