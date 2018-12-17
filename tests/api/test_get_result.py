import json
from io import BytesIO

import pytest
from minio import Minio

from shepherd.constants import JOB_STATUS_FILE, OUTPUT_DIR
from shepherd.api.models import JobStatus


@pytest.fixture()
def job_done(minio: Minio, bucket):
    job_id = bucket
    status = json.dumps({
        "status": JobStatus.DONE,
        "model": {
            "name": "dfsdf",
            "version": "adfk"
        },
        "finished_at": '2000-06-10T12:15:30.005000'
    }).encode()
    minio.put_object(job_id, JOB_STATUS_FILE, BytesIO(status), len(status))
    data = json.dumps({"content": "Lorem ipsum"}).encode()
    minio.put_object(job_id, OUTPUT_DIR + "/payload.json", BytesIO(data), len(data))
    yield job_id


@pytest.fixture()
def job_failed(minio: Minio, bucket):
    job_id = bucket
    status = json.dumps({
        "status": JobStatus.FAILED,
        "model": {
            "name": "dfsdf",
            "version": "adfk"
        },
        "error_details": {
            "message": "General error"
        },
        "finished_at": '2000-06-10T12:15:30.005000'
    }).encode()
    minio.put_object(job_id, JOB_STATUS_FILE, BytesIO(status), len(status))
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
