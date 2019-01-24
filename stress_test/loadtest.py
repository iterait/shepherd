from uuid import uuid4 as uuid
import json
import asyncio
from io import BytesIO
from time import sleep
from minio import Minio

from molotov import scenario

from shepherd.constants import DEFAULT_PAYLOAD_PATH

_SHEPHERD_URL = 'http://0.0.0.0:5000'
_BYTES = str(open("/dev/urandom", "rb").read(200000))
_SLEEP = 5


def minio():
    minio_client = Minio(
        'localhost:7000',
        access_key='AKIAIOSFODNN7EXAMPLE',
        secret_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        secure=False
    )

    return minio_client


def create_payload(job_id: str):
    return {
        'job_id': job_id,
        'payload': json.dumps(dict(
            input='42'
        )),
        'model': {'name': 'stress_test', 'version': ''}
    }


def create_huge_payload(job_id: str):
    return {
        'job_id': job_id,
        'payload': json.dumps(dict(
            input=_BYTES
        )),
        'model': {'name': 'stress_test', 'version': ''}
    }


def create_payload_minio(job_id: str):
    return {
        'job_id': job_id,
        'model': {'name': 'stress_test', 'version': ''}
    }


async def job_lifecycle(session):
    job_id = str(uuid())
    json_dict = create_payload(job_id)

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
            else:
                assert resp.status == 202, 'shepherd exploded!'


async def job_lifecycle_minio(session, minio_client):
    job_id = str(uuid())

    payload = b'{"input":"42"}'
    minio_client.make_bucket(job_id)
    minio_client.put_object(job_id, DEFAULT_PAYLOAD_PATH, BytesIO(payload), len(payload))

    json_dict = create_payload_minio(job_id)

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


async def job_lifecycle_huge_payload(session):
    job_id = str(uuid())
    json_dict = create_huge_payload(job_id)

    async with session.post(f'{_SHEPHERD_URL}/start-job', json=json_dict) as resp:
        assert resp.status == 200

    async with session.get(f'{_SHEPHERD_URL}/jobs/{job_id}/status') as resp:
        assert resp.status == 200

    while True:
        async with session.get(f'{_SHEPHERD_URL}/jobs/{job_id}/result') as resp:
            if resp.status == 200:
                response = await resp.text()
                response = json.loads(response)
                assert response['result'] == _BYTES
                break
            else:
                assert resp.status == 202, 'shepherd exploded!'


async def job_lifecycle_wait(session):
    job_id = str(uuid())
    json_dict = create_payload(job_id)

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


@scenario(weight=20)
async def job_with_payload(session):
    await asyncio.wait_for(job_lifecycle(session), timeout=120)


@scenario(weight=20)
async def job_with_minio(session):
    await asyncio.wait_for(job_lifecycle_minio(session, minio()), timeout=120)


@scenario(weight=20)
async def huge_job(session):
    await asyncio.wait_for(job_lifecycle_huge_payload(session), timeout=600)


@scenario(weight=20)
async def job_with_wait(session):
    await asyncio.wait_for(job_lifecycle_wait(session), timeout=120)


@scenario(weight=5)
async def nonexistent_job(session):
    job_id = 'non-existent'

    async with session.get(f'{_SHEPHERD_URL}/jobs/{job_id}/status') as resp:
        assert resp.status == 400

    async with session.get(f'{_SHEPHERD_URL}/jobs/{job_id}/result') as resp:
        response = await resp.text()
        response = json.loads(response)
        assert response['message'] == f'Data for job `{job_id}` does not exist'

    sleep(_SLEEP)


@scenario(weight=5)
async def shepherd_status(session):
    async with session.get(f'{_SHEPHERD_URL}/status') as resp:
        assert resp.status == 200

    sleep(_SLEEP)
