import json
from io import BytesIO
from datetime import datetime, timedelta
import pytest

from shepherd.constants import DONE_FILE


def test_get_status(client):
    response = client.get('/status')
    assert response.status_code == 200
    assert response.json['containers'] == {'bare_sheep': {'running': False,
                                                          'request': None,
                                                          'model': {'name': 'model_1',
                                                                    'version': 'latest'}}}


def test_ready(client, minio):
    response = client.post('/start-job', content_type='application/json', data=json.dumps({
        'job_id': 'uuid-ready',
        'sheep_id': 'bare_sheep',
        'model': {
            'name': 'model_1',
            'version': 'latest'
        },
        'payload': 'Payload content'
    }))
    assert response.status_code == 200
    minio.put_object('uuid-ready', DONE_FILE, BytesIO(), 0)
    timestamp = datetime.now()

    response = client.get('/jobs/uuid-ready/wait_ready')
    assert response.status_code == 200
    assert response.json == {'ready': True}

    response = client.get('/jobs/uuid-ready/ready')
    assert response.status_code == 200
    assert response.json['ready'] is True
    timestamp_diff = timestamp - datetime.strptime(response.json['finished_at'], '%Y-%m-%dT%H:%M:%S.%f')
    assert timestamp_diff < timedelta(seconds=1)

    response = client.post('/start-job', content_type='application/json', data=json.dumps({
        'job_id': 'uuid-not-ready',
        'sheep_id': 'bare_sheep',
        'model': {
            'name': 'model_1',
            'version': 'latest'
        },
        'payload': 'Payload content'
    }))
    assert response.status_code == 200

    response = client.get('/jobs/uuid-not-ready/wait_ready')
    assert response.status_code == 200
    assert response.json == {'ready': False}

    response = client.get('/jobs/uuid-not-ready/ready')
    assert response.status_code == 200
    assert response.json == {'ready': False,
                             'finished_at': None}


def test_ready_error(client):
    response = client.get('/jobs/non-existent/wait_ready')
    assert response.status_code == 400

    response = client.get('/jobs/non-existent/ready')
    assert response.status_code == 400
