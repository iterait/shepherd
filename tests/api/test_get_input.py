import json
from io import BytesIO

import pytest
from minio import Minio

from shepherd.constants import INPUT_DIR


@pytest.fixture()
def job(minio: Minio, bucket):
    job_id = bucket
    data = json.dumps({"content": "Lorem ipsum"}).encode()
    minio.put_object(job_id, INPUT_DIR + "/payload.json", BytesIO(data), len(data))
    yield job_id


async def test_get_result_success(job, aiohttp_client, app):
    job_id = job
    client = await aiohttp_client(app)
    response = await client.get("/jobs/{}/input/payload.json".format(job_id))
    assert response.status == 200

    data = await response.json()
    assert "content" in data


async def test_get_result_not_found(job, aiohttp_client, app):
    job_id = job
    client = await aiohttp_client(app)

    response = await client.get("/jobs/{}/input/i-dont-exist.json".format(job_id))
    assert response.status == 404
