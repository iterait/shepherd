from uuid import uuid4 as uuid
import json
import requests

_SHEPHERD_URL = 'http://0.0.0.0:5000'


def create_payload(i: int, job_id: str):
    return {
        'job_id': job_id,
        'payload': json.dumps(dict(
            sleep=str(i % 5),
            input='42'
        )),
        'model': {'name': 'stress_test', 'version': ''}
    }


def tiny_test() -> None:
    job_ids = []

    for i in range(10):
        job_id = str(uuid())
        json_dict = create_payload(i, job_id)
        response = requests.post(f'{_SHEPHERD_URL}/start-job', json=json_dict)
        assert response.status_code == 200
        job_ids.append(job_id)

    for job_id in job_ids:
        status = requests.get(f'{_SHEPHERD_URL}/jobs/{job_id}/status')
        assert status.status_code == 200

        result = requests.get(f'{_SHEPHERD_URL}/jobs/{job_id}/result')
        while result.status_code == 202:
            result = requests.get(f'{_SHEPHERD_URL}/jobs/{job_id}/result')
        assert result.json()['result'] == '42'
