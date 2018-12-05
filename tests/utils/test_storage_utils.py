import pytest
import os
import os.path as path
import io

from minio import Minio

from shepherd.config import StorageConfig
from shepherd.constants import INPUT_DIR, OUTPUT_DIR
from shepherd.storage import MinioStorage
from shepherd.utils import *
from shepherd.errors.api import StorageError, StorageInaccessibleError


@pytest.fixture()
async def storage(storage_config: StorageConfig, loop):
    yield MinioStorage(storage_config)


@pytest.fixture()
def job_dir(tmpdir, bucket):
    dir_path = path.join(tmpdir, bucket)
    create_clean_dir(dir_path)
    yield dir_path


@pytest.fixture()
def storage_config_inaccessible(aiohttp_unused_port):
    yield StorageConfig({
        'url': f'http://0.0.0.0:{aiohttp_unused_port()}',
        'access_key': 'AKIAIOSFODNN7EXAMPLE',
        'secret_key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    })


def test_create_clean_dir(tmpdir):
    # check directory does not exist
    target_dir = path.join(tmpdir, 'my_dir')
    assert not path.exists(target_dir)

    # create clean dir and check if it exists
    clean_dir = create_clean_dir(target_dir)
    assert clean_dir == target_dir
    assert path.exists(target_dir) and path.isdir(target_dir)
    assert len(os.listdir(target_dir)) == 0

    # create a file
    with open(path.join(clean_dir, 'file.txt'), 'w') as file:
        file.write('content')
    assert len(os.listdir(target_dir)) == 1

    # create it again (it should be cleaned)
    create_clean_dir(clean_dir)
    assert len(os.listdir(target_dir)) == 0


def test_minio_connectivity(minio: Minio):
    assert not minio.list_buckets()  # we expect an empty minio


async def test_minio_push(storage: MinioStorage, minio: Minio, bucket, job_dir):
    inputs_dir = create_clean_dir(path.join(job_dir, OUTPUT_DIR))
    another_dir = create_clean_dir(path.join(job_dir, 'another'))

    # create two files
    for dir_ in (inputs_dir, another_dir):
        with open(path.join(inputs_dir, 'file.txt'), 'w') as file:
            file.write(f'{dir_}content')

    # test if only the one in the outputs folder is pushed
    await storage.push_job_data(bucket, job_dir)
    minio_objects = list(minio.list_objects_v2(bucket, recursive=True))
    assert len(minio_objects) == 1
    assert minio_objects[0].object_name == OUTPUT_DIR + '/file.txt'

    assert minio_object_exists(minio, bucket, OUTPUT_DIR + '/file.txt')
    assert not minio_object_exists(minio, bucket, 'another/file.txt')


async def test_minio_push_empty(storage: MinioStorage, bucket, job_dir, caplog, minio):
    # test warning
    await storage.push_job_data(bucket, job_dir)
    assert 'No output files pushed to bucket' in caplog.text


async def test_minio_push_missing(storage: MinioStorage, job_dir, bucket, minio):
    with pytest.raises(StorageError):
        await storage.push_job_data(f'{bucket}-missing', job_dir)


async def test_minio_pull(storage: MinioStorage, minio: Minio, bucket, job_dir):
    data = b'some data'
    minio.put_object(bucket, INPUT_DIR + '/file.dat', io.BytesIO(data), len(data))
    minio.put_object(bucket, 'another/file.dat', io.BytesIO(data), len(data))

    assert minio_object_exists(minio, bucket, INPUT_DIR + '/file.dat')
    assert minio_object_exists(minio, bucket, 'another/file.dat')

    await storage.pull_job_data(bucket, job_dir)
    filepath = path.join(job_dir, INPUT_DIR, 'file.dat')
    assert path.exists(filepath)
    with open(filepath) as file:
        assert file.read() == 'some data'
    assert not path.exists(path.join(job_dir, 'another', 'file.dat'))


async def test_minio_pull_empty(storage: MinioStorage, job_dir, bucket, caplog, minio):
    await storage.pull_job_data(bucket, job_dir)
    assert 'No input objects pulled from bucket' in caplog.text


async def test_minio_pull_missing(storage: MinioStorage, bucket, job_dir, minio):
    with pytest.raises(StorageError):
        await storage.pull_job_data(f'{bucket}-missing', job_dir)


async def test_minio_pull_inaccessible(job_dir, storage_config_inaccessible, minio):
    storage = MinioStorage(storage_config_inaccessible)

    with pytest.raises(StorageInaccessibleError):
        await storage.pull_job_data(f'whatever', job_dir)


async def test_minio_accessibility_positive(storage: MinioStorage, minio):
    assert await storage.is_accessible()


async def test_minio_accessibility_negative(storage_config_inaccessible, minio):
    storage = MinioStorage(storage_config_inaccessible)
    assert not await storage.is_accessible()


async def test_nonexistent_job_done(storage: MinioStorage, minio):
    assert not await storage.is_job_done("whatever-i-dont-exist")
