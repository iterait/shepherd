import json
from io import BytesIO

import pytest
from minio import Minio

from shepherd.constants import DONE_FILE, ERROR_FILE, OUTPUT_DIR


@pytest.fixture()
def job_done(minio: Minio, bucket):
    job_id = bucket
    minio.put_object(job_id, DONE_FILE, BytesIO(), 0)
    data = json.dumps({"content": "Lorem ipsum"}).encode()
    minio.put_object(job_id, OUTPUT_DIR + "/payload.json", BytesIO(data), len(data))
    yield job_id


@pytest.fixture()
def job_failed(minio: Minio, bucket):
    job_id = bucket
    err_msg = b"General error"
    minio.put_object(job_id, ERROR_FILE, BytesIO(err_msg), len(err_msg))
    yield job_id


async def test_get_result_success(job_done, aiohttp_client, app):
    job_id = job_done
    client = await aiohttp_client(app)
    response = await client.get("/jobs/{}/result/payload.json".format(job_id))
    assert response.status == 200

    data = await response.json()
    assert "content" in data


async def test_get_result_not_ready(bucket, aiohttp_client, app):
    job_id = bucket
    client = await aiohttp_client(app)

    response = await client.get("/jobs/{}/result/payload.json".format(job_id))
    assert response.status == 202


async def test_get_result_error(job_failed, aiohttp_client, app):
    job_id = job_failed
    client = await aiohttp_client(app)

    response = await client.get("/jobs/{}/result/payload.json".format(job_id))

    assert response.status == 500
    data = await response.json()
    assert data["message"] == "General error"


async def test_get_result_not_found(job_done, aiohttp_client, app):
    job_id = job_done
    client = await aiohttp_client(app)

    response = await client.get("/jobs/{}/result/i-dont-exist.json".format(job_id))
    assert response.status == 404
