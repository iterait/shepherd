import json
from io import BytesIO
from typing import Union

from minio import Minio
from unittest.mock import Mock
from werkzeug.test import Client

from cxworker.api.errors import UnknownJobError

from cxworker.constants import DEFAULT_PAYLOAD_PATH
from cxworker.shepherd import Shepherd


def test_start_job_with_payload(minio_scoped: Minio, client: Client, mock_shepherd: Union[Mock, Shepherd]):
    mock_shepherd.is_job_done.side_effect = UnknownJobError()

    response = client.post("/start-job", content_type="application/json", data=json.dumps({
        "job_id": "uuid-1",
        "sheep_id": "sheep_1",
        "model": {
            "name": "model_1",
            "version": "latest"
        },
        "payload": "Payload content"
    }))

    assert response.status_code == 200
    assert minio_scoped.bucket_exists("uuid-1")

    payload = minio_scoped.get_object("uuid-1", DEFAULT_PAYLOAD_PATH)
    assert payload.data == b"Payload content"

    mock_shepherd.enqueue_job.assert_called()


def test_start_job_with_payload_conflict(minio_scoped: Minio, client: Client, mock_shepherd: Union[Mock, Shepherd]):
    mock_shepherd.is_job_done.side_effect = UnknownJobError()

    minio_scoped.make_bucket("uuid-1")

    response = client.post("/start-job", content_type="application/json", data=json.dumps({
        "job_id": "uuid-1",
        "sheep_id": "sheep_1",
        "model": {
            "name": "model_1",
            "version": "latest"
        },
        "payload": "Payload content"
    }))

    assert response.status_code == 409

    mock_shepherd.enqueue_job.assert_not_called()


def test_start_job_no_payload(client):
    response = client.post("/start-job", content_type="application/json", data=json.dumps({
        "job_id": "uuid-2",
        "sheep_id": "sheep_1",
        "model": {
            "name": "model_1",
            "version": "latest"
        }
    }))

    assert response.status_code == 400  # Neither the request nor minio contains a payload -> error


def test_start_job_with_payload_in_minio(minio_scoped: Minio, client: Client, mock_shepherd: Union[Mock, Shepherd]):
    mock_shepherd.is_job_done.side_effect = UnknownJobError()

    payload = b"Payload content"
    minio_scoped.make_bucket("uuid-3")
    minio_scoped.put_object("uuid-3", "payload.json", BytesIO(payload), len(payload))

    response = client.post("/start-job", content_type="application/json", data=json.dumps({
        "job_id": "uuid-3",
        "sheep_id": "sheep_1",
        "model": {
            "name": "model_1",
            "version": "latest"
        }
    }))

    assert response.status_code == 200

    mock_shepherd.enqueue_job.assert_called()
