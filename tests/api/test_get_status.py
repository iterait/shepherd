import json


def test_get_status(client):
    response = client.get('/status')
    assert response.status_code == 200
    assert response.json['containers'] == {'bare_sheep': {'running': False,
                                                          'request': None,
                                                          'model': {'name': 'model_1',
                                                                    'version': 'latest'}}}


def test_ready(client):
    response = client.post('/start-job', content_type='application/json', data=json.dumps({
        'job_id': 'uuid-5',
        'sheep_id': 'bare_sheep',
        'model': {
            'name': 'model_1',
            'version': 'latest'
        },
        'payload': 'Payload content'
    }))

    assert response.status_code == 200

    response = client.get('/jobs/uuid-5/wait_ready')
    assert response.status_code == 200
    assert response.json == {'ready': True}

    response = client.get('/jobs/uuid-5/ready')
    assert response.status_code == 200
    assert response.json == {'ready': True}


def test_ready_error(client):
    response = client.get('/jobs/non-existent/wait_ready')
    assert response.status_code == 400

    response = client.get('/jobs/non-existent/ready')
    assert response.status_code == 400
