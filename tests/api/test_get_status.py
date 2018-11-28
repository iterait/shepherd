import json
import pytest
from io import BytesIO
from datetime import datetime, timedelta

from shepherd.constants import DONE_FILE


async def test_get_status(aiohttp_client, app):
    client = await aiohttp_client(app)
    response = await client.get('/status')
    assert response.status == 200
    data = await response.json()
    assert data['containers'] == {'bare_sheep': {'running': False,
                                                 'request': None,
                                                 'model': {'name': 'model_1',
                                                           'version': 'latest'}}}


async def test_ready(aiohttp_client, minio_scoped, app):
    minio = minio_scoped

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
    minio.put_object('uuid-ready', DONE_FILE, BytesIO(), 0)
    timestamp = datetime.now()

    response = await client.get('/jobs/uuid-ready/wait_ready')
    assert response.status == 200
    data = await response.json()
    assert data == {'ready': True}

    response = await client.get('/jobs/uuid-ready/ready')
    assert response.status == 200
    data = await response.json()
    assert data['ready'] is True
    timestamp_diff = timestamp - datetime.strptime(data['finished_at'], '%Y-%m-%dT%H:%M:%S.%f')
    assert timestamp_diff < timedelta(seconds=1)


@pytest.mark.skip
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

    response = await client.get('/jobs/uuid-not-ready/wait_ready')
    data = await response.json()
    assert response.status == 200
    assert data == {'ready': False}  # TODO how is this supposed to happen??

    response = await client.get('/jobs/uuid-not-ready/ready')
    data = await response.json()
    assert response.status == 200
    assert data == {'ready': False,
                    'finished_at': None}


async def test_ready_error(aiohttp_client, app):
    client = await aiohttp_client(app)
    response = await client.get('/jobs/non-existent/wait_ready')
    assert response.status == 400

    response = await client.get('/jobs/non-existent/ready')
    assert response.status == 400
