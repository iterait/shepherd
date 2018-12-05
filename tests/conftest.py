import asyncio

import aiobotocore
import logging
import pytest
from minio import Minio
import subprocess
import os
import random
import string
from typing import Tuple

from shepherd.config import RegistryConfig, StorageConfig


@pytest.fixture()
def registry_config():
    yield RegistryConfig(dict(url='https://registry.hub.docker.com',
                              username='iteraitshepherd',
                              password='Iterait123'))


@pytest.fixture(scope='session')
def storage_config():
    yield StorageConfig({
        'url': 'http://0.0.0.0:7000',
        'access_key': 'AKIAIOSFODNN7EXAMPLE',
        'secret_key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    })


@pytest.fixture(scope='session')
def minio(storage_config: StorageConfig, tmpdir_factory):
    try:
        if subprocess.call(['minio']) != 0:
            raise RuntimeError()
    except:
        pytest.skip("Minio is not installed")

    data_dir = tmpdir_factory.mktemp('minio')

    assert len(os.listdir(data_dir)) == 0

    env = os.environ.copy()
    env['MINIO_ACCESS_KEY'] = storage_config.access_key
    env['MINIO_SECRET_KEY'] = storage_config.secret_key
    proc = subprocess.Popen(['minio', 'server', '--address', storage_config.schemeless_url, data_dir], env=env)
    yield Minio(
        storage_config.schemeless_url,
        access_key=storage_config.access_key,
        secret_key=storage_config.secret_key,
        secure=False
    )
    proc.kill()


@pytest.fixture(scope='function')
def minio_scoped(minio: Minio):
    assert not minio.list_buckets()

    yield minio

    for bucket in minio.list_buckets():
        for obj in minio.list_objects_v2(bucket.name, recursive=True):
            minio.remove_object(obj.bucket_name, obj.object_name)
        minio.remove_bucket(bucket.name)


@pytest.fixture()
def bucket(minio: Minio):
    request_id = 'test-request-' + (''.join(random.choices(string.ascii_lowercase + string.digits, k=10)))
    if minio.bucket_exists(request_id):
        for obj in minio.list_objects_v2(request_id, recursive=True):
            minio.remove_object(request_id, obj.object_name)
        minio.remove_bucket(request_id)
    minio.make_bucket(request_id)
    assert not list(minio.list_objects_v2(request_id, recursive=True))

    yield request_id

    if minio.bucket_exists(request_id):
        for obj in minio.list_objects_v2(request_id, recursive=True):
            minio.remove_object(request_id, obj.object_name)
        minio.remove_bucket(request_id)

    assert not minio.bucket_exists(request_id)


@pytest.fixture()
def image_valid() -> Tuple[str, str]:
    yield 'library/alpine', 'latest'


@pytest.fixture()
def image_invalid() -> Tuple[str, str]:
    yield 'iterait/non-existing-image', 'latest'
