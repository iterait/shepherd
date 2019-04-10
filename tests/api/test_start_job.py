import json
from io import BytesIO
from typing import Union

import pytest
from minio import Minio
from unittest.mock import Mock

from shepherd.errors.api import UnknownJobError

from shepherd.constants import DEFAULT_PAYLOAD_PATH
from shepherd.shepherd import Shepherd


async def unknown_job_coroutine(job_id):
    raise UnknownJobError()


async def test_start_job_with_payload(minio: Minio, aiohttp_client, app, mock_shepherd: Union[Mock, Shepherd]):
    mock_shepherd.is_job_done.side_effect = unknown_job_coroutine
    client = await aiohttp_client(app)

    response = await client.post("/start-job", headers={"Content-Type": "application/json"}, data=json.dumps({
        "job_id": "uuid-1",
        "sheep_id": "sheep_1",
        "model": {
            "name": "model_1",
            "version": "latest"
        },
        "payload": "Payload content"
    }))

    assert response.status == 200
    assert minio.bucket_exists("uuid-1")

    payload = minio.get_object("uuid-1", DEFAULT_PAYLOAD_PATH)
    assert payload.data == b"Payload content"

    mock_shepherd.enqueue_job.assert_called()


async def test_start_job_with_payload_conflict(minio: Minio, aiohttp_client, app, mock_shepherd: Union[Mock, Shepherd]):
    mock_shepherd.is_job_done.side_effect = unknown_job_coroutine
    client = await aiohttp_client(app)

    minio.make_bucket("uuid-1")

    response = await client.post("/start-job", headers={"Content-Type": "application/json"}, data=json.dumps({
        "job_id": "uuid-1",
        "sheep_id": "sheep_1",
        "model": {
            "name": "model_1",
            "version": "latest"
        },
        "payload": "Payload content"
    }))

    assert response.status == 409

    mock_shepherd.enqueue_job.assert_not_called()


async def test_start_job_no_payload(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.post("/start-job", headers={"Content-Type": "application/json"}, data=json.dumps({
        "job_id": "uuid-2",
        "sheep_id": "sheep_1",
        "model": {
            "name": "model_1",
            "version": "latest"
        }
    }))

    assert response.status == 400  # Neither the request nor minio contains a payload -> error


async def test_start_job_not_json(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.post("/start-job", headers={"Content-Type": "application/json"}, data='not-json')

    assert response.status == 400


async def test_start_job_missing_model(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.post("/start-job", headers={"Content-Type": "application/json"}, data=json.dumps({
        "job_id": "uuid-1",
        "sheep_id": "sheep_1",
        "payload": "Payload content"
    }))

    assert response.status == 400


async def test_start_job_with_payload_in_minio(minio: Minio, aiohttp_client, app, mock_shepherd: Union[Mock, Shepherd]):
    client = await aiohttp_client(app)
    mock_shepherd.is_job_done.side_effect = unknown_job_coroutine

    payload = b"Payload content"
    minio.make_bucket("uuid-3")
    minio.put_object("uuid-3", "payload.json", BytesIO(payload), len(payload))

    response = await client.post("/start-job", headers={"Content-Type": "application/json"}, data=json.dumps({
        "job_id": "uuid-3",
        "sheep_id": "sheep_1",
        "model": {
            "name": "model_1",
            "version": "latest"
        }
    }))

    assert response.status == 200

    mock_shepherd.enqueue_job.assert_called()
