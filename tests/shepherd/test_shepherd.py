import asyncio
import json
from contextlib import suppress

import pytest

from shepherd.constants import DEFAULT_OUTPUT_PATH, JOB_STATUS_FILE
from shepherd.sheep import BareSheep, DockerSheep
from shepherd.api.models import JobStatus
from shepherd.shepherd import Shepherd
from shepherd.errors.api import UnknownSheepError, UnknownJobError
from shepherd.errors.sheep import SheepConfigurationError
from shepherd.config import ShepherdConfig
from shepherd.utils.storage import minio_object_exists


async def test_shepherd_init(valid_config: ShepherdConfig, minio):

    # test valid shepherd creation
    shepherd = Shepherd(valid_config.sheep, valid_config.data_root, minio, valid_config.registry)
    assert isinstance(shepherd._get_sheep('bare_sheep'), BareSheep)

    # test sheep getter
    with pytest.raises(UnknownSheepError):
        _ = shepherd._get_sheep('UnknownSheep')

    # test multiple sheep and created sheep type
    valid_config.sheep['docker_sheep'] = {'port': 9002, 'type': 'docker'}
    shepherd = Shepherd(valid_config.sheep, valid_config.data_root, minio, valid_config.registry)
    assert isinstance(shepherd._get_sheep('docker_sheep'), DockerSheep)

    # test missing docker registry raises an Exception when creating docker sheep
    with pytest.raises(SheepConfigurationError):
        Shepherd(valid_config.sheep, valid_config.data_root, minio)

    # test unknown sheep type raises an Exception
    valid_config.sheep['my_sheep'] = {'type': 'unknown'}
    with pytest.raises(SheepConfigurationError):
        Shepherd(valid_config.sheep, valid_config.data_root, minio, valid_config.registry)


async def test_shepherd_status(shepherd):
    sheep_name, sheep = next(shepherd.get_status())
    assert not sheep.running
    assert sheep_name == 'bare_sheep'


async def test_job_unknown(minio, shepherd):
    with pytest.raises(UnknownJobError):
        await shepherd.is_job_done("unknown-job")


async def wait_for_job(shepherd: Shepherd, job_id: str):
    async with shepherd.job_done_condition:
        for _ in range(10):
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(shepherd.job_done_condition.wait(), 2)

            if await shepherd.is_job_done(job_id):
                return


async def test_job(job, shepherd: Shepherd, minio):
    job_id, job_meta = job

    await shepherd.enqueue_job(job_id, job_meta)
    assert not await shepherd.is_job_done(job_id)

    await wait_for_job(shepherd, job_id)

    assert shepherd._get_sheep('bare_sheep').running
    assert await shepherd.is_job_done(job_id)
    assert minio_object_exists(minio, job_id, DEFAULT_OUTPUT_PATH)
    assert minio_object_exists(minio, job_id, JOB_STATUS_FILE)
    output = json.loads(minio.get_object(job_id, DEFAULT_OUTPUT_PATH).read().decode())
    assert output['key'] == [1000]
    assert output['output'] == [1000*2]


async def test_failed_job(bad_job, minio, shepherd: Shepherd):
    job_id, job_meta = bad_job
    await shepherd.enqueue_job(job_id, job_meta)  # runner should fail to process the job (and send an ErrorMessage)

    await wait_for_job(shepherd, job_id)

    assert shepherd._get_sheep('bare_sheep').running
    assert await shepherd.is_job_done(job_id)
    assert minio_object_exists(minio, job_id, JOB_STATUS_FILE)
    assert json.load(minio.get_object(job_id, JOB_STATUS_FILE))["status"] == JobStatus.FAILED


async def test_bad_configuration_job(shepherd, bad_configuration_job, minio):
    job_id, job_meta = bad_configuration_job
    await shepherd.enqueue_job(job_id, job_meta)  # shepherd should get SheepConfigurationError
    await asyncio.sleep(1)
    assert not shepherd._get_sheep('bare_sheep').running
    assert await shepherd.is_job_done(job_id)
    assert minio_object_exists(minio, job_id, JOB_STATUS_FILE)
    assert json.load(minio.get_object(job_id, JOB_STATUS_FILE))["status"] == JobStatus.FAILED


async def test_bad_runner_job(shepherd, bad_runner_job, minio):
    job_id, job_meta = bad_runner_job
    await shepherd.enqueue_job(job_id, job_meta)  # runner should not start (and health-check should discover it)
    await asyncio.sleep(3)
    assert not shepherd._get_sheep('bare_sheep').running
    print(json.load(minio.get_object(job_id, JOB_STATUS_FILE)))
    assert await shepherd.is_job_done(job_id)
    assert minio_object_exists(minio, job_id, JOB_STATUS_FILE)
    assert json.load(minio.get_object(job_id, JOB_STATUS_FILE))["status"] == JobStatus.FAILED
