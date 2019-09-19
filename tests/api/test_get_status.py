import json
import asyncio
import pytest
from io import BytesIO

from shepherd.constants import JOB_STATUS_FILE
from shepherd.api.models import JobStatus


async def test_get_status(aiohttp_client, app):
    client = await aiohttp_client(app)
    response = await client.get('/status')
    assert response.status == 200
    data = await response.json()
    assert data['sheep'] == {'bare_sheep': {'running': False,
                                                 'request': None,
                                                 'model': {'name': 'model_1',
                                                           'version': 'latest'}}}


async def test_ready(aiohttp_client, minio, app):
    client = await aiohttp_client(app)
    response = await client.post('/start-job', headers={'Content-Type': 'application/json'}, data=json.dumps({
        'job_id': 'uuid-ready',
        'sheep_id': 'bare_sheep',
        'model': {
            'name': 'model_1',
            'version': 'latest'
        },
        'payload': 'Payload content'
    }))
    assert response.status == 200

    status = json.dumps({
        "status": JobStatus.DONE,
        "finished_at": '2000-06-10T12:15:30.005000',
        "model": {
            "name": "abcd",
            "version": "abcd"
        }
    }).encode()

    minio.put_object('uuid-ready', JOB_STATUS_FILE, BytesIO(status), len(status))

    response = await client.get('/jobs/uuid-ready/wait_ready')
    assert response.status == 200
    data = await response.json()
    assert data["status"] == JobStatus.DONE


async def test_not_ready(aiohttp_client, app):
    client = await aiohttp_client(app)
    response = await client.post('/start-job', headers={'Content-Type': 'application/json'}, data=json.dumps({
        'job_id': 'uuid-not-ready',
        'sheep_id': 'bare_sheep',
        'model': {
            'name': 'model_1',
            'version': 'latest'
        },
        'payload': 'Payload content'
    }))
    assert response.status == 200

    with pytest.raises(asyncio.TimeoutError):
        response = await asyncio.wait_for(client.get('/jobs/uuid-not-ready/wait_ready'), timeout=1)
        assert response.status == 200
        data = await response.json()
        assert data['status'] == JobStatus.PROCESSING


async def test_ready_error(aiohttp_client, app):
    client = await aiohttp_client(app)
    response = await client.get('/jobs/non-existent/wait_ready')
    assert response.status == 400
