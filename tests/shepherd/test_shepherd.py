import asyncio
import json

import pytest

from shepherd.constants import ERROR_FILE, DEFAULT_OUTPUT_PATH, DONE_FILE
from shepherd.sheep import BareSheep, DockerSheep
from shepherd.shepherd import Shepherd
from shepherd.errors.api import UnknownSheepError, UnknownJobError
from shepherd.errors.sheep import SheepConfigurationError
from shepherd.config import ShepherdConfig
from shepherd.utils.storage import minio_object_exists


async def test_shepherd_init(valid_config: ShepherdConfig, minio):

    # test valid shepherd creation
    shepherd = Shepherd(valid_config.sheep, valid_config.data_root, minio, valid_config.registry)
    assert isinstance(shepherd['bare_sheep'], BareSheep)

    # test sheep getter
    with pytest.raises(UnknownSheepError):
        _ = shepherd['UnknownSheep']

    # test multiple sheep and created sheep type
    valid_config.sheep['docker_sheep'] = {'port': 9002, 'type': 'docker'}
    shepherd = Shepherd(valid_config.sheep, valid_config.data_root, minio, valid_config.registry)
    assert isinstance(shepherd['docker_sheep'], DockerSheep)

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


async def test_job(shepherd: Shepherd, job, minio, loop):
    job_id, job_meta = job

    with pytest.raises(UnknownJobError):
        shepherd.is_job_done(job_id)

    await shepherd.enqueue_job(job_id, job_meta)
    assert not shepherd.is_job_done(job_id)

    async with shepherd.job_done_condition:
        await shepherd.job_done_condition.wait_for(lambda: shepherd.is_job_done(job_id))

    assert shepherd['bare_sheep'].running
    assert shepherd.is_job_done(job_id)
    assert minio_object_exists(minio, job_id, DEFAULT_OUTPUT_PATH)
    assert minio_object_exists(minio, job_id, DONE_FILE)
    output = json.loads(minio.get_object(job_id, DEFAULT_OUTPUT_PATH).read().decode())
    assert output['key'] == [1000]
    assert output['output'] == [1000*2]


async def test_failed_job(shepherd, bad_job, minio):
    job_id, job_meta = bad_job
    await shepherd.enqueue_job(job_id, job_meta)  # runner should fail to process the job (and send an ErrorMessage)
    async with shepherd.job_done_condition:
        await shepherd.job_done_condition.wait_for(lambda: shepherd.is_job_done(job_id))
    assert shepherd['bare_sheep'].running
    assert shepherd.is_job_done(job_id)
    assert minio_object_exists(minio, job_id, ERROR_FILE)


async def test_bad_configuration_job(shepherd, bad_configuration_job, minio):
    job_id, job_meta = bad_configuration_job
    await shepherd.enqueue_job(job_id, job_meta)  # shepherd should get SheepConfigurationError
    await asyncio.sleep(1)
    assert not shepherd['bare_sheep'].running
    assert shepherd.is_job_done(job_id)
    assert minio_object_exists(minio, job_id, ERROR_FILE)


async def test_bad_runner_job(shepherd, bad_runner_job, minio):
    job_id, job_meta = bad_runner_job
    await shepherd.enqueue_job(job_id, job_meta)  # runner should not start (and health-check should discover it)
    await asyncio.sleep(3)
    assert not shepherd['bare_sheep'].running
    assert shepherd.is_job_done(job_id)
    assert minio_object_exists(minio, job_id, ERROR_FILE)
