from uuid import uuid4 as uuid
import json
import random
import asyncio

from molotov import scenario

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


async def job_lifecycle(session):
    job_id = str(uuid())
    i = random.randint(1, 5)
    json_dict = create_payload(i, job_id)

    async with session.post(f'{_SHEPHERD_URL}/start-job', json=json_dict) as resp:
        assert resp.status == 200

    async with session.get(f'{_SHEPHERD_URL}/jobs/{job_id}/status') as resp:
        assert resp.status == 200

    while True:
        async with session.get(f'{_SHEPHERD_URL}/jobs/{job_id}/result') as resp:
            if resp.status == 200:
                response = await resp.text()
                response = json.loads(response)
                assert response['result'] == '42'
                break
            elif resp.status != 202:
                raise ValueError('shepherd exploded!')


async def job_lifecycle_wait(session):
    job_id = str(uuid())
    i = random.randint(1, 5)
    json_dict = create_payload(i, job_id)

    async with session.post(f'{_SHEPHERD_URL}/start-job', json=json_dict) as resp:
        assert resp.status == 200

    async with session.get(f'{_SHEPHERD_URL}/jobs/{job_id}/status') as resp:
        assert resp.status == 200

    async with session.get(f'{_SHEPHERD_URL}/jobs/{job_id}/wait_ready') as resp:
        assert resp.status == 200

    async with session.get(f'{_SHEPHERD_URL}/jobs/{job_id}/result') as resp:
        response = await resp.text()
        response = json.loads(response)
        assert response['result'] == '42'


@scenario(weight=55)
async def job_scenario(session):
    await asyncio.wait_for(job_lifecycle(session), timeout=120)


@scenario(weight=30)
async def job_scenario_wait(session):
    await asyncio.wait_for(job_lifecycle_wait(session), timeout=120)


@scenario(weight=10)
async def nonexistent_scenario(session):
    job_id = 'non-existent'

    async with session.get(f'{_SHEPHERD_URL}/jobs/{job_id}/status') as resp:
        assert resp.status == 400

    async with session.get(f'{_SHEPHERD_URL}/jobs/{job_id}/result') as resp:
        response = await resp.text()
        response = json.loads(response)
        assert response['message'] == f'Data for job `{job_id}` does not exist'


@scenario(weight=5)
async def shepherd_status(session):
    async with session.get(f'{_SHEPHERD_URL}/status') as resp:
        assert resp.status == 200
