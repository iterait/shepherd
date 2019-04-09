import pytest
import subprocess
import os
import random
import string
from typing import Tuple

from minio import Minio

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
def minio_connection(storage_config: StorageConfig, tmpdir_factory):
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
def minio(minio_connection: Minio):
    assert not minio_connection.list_buckets()

    yield minio_connection

    for bucket in minio_connection.list_buckets():
        for obj in minio_connection.list_objects_v2(bucket.name, recursive=True):
            minio_connection.remove_object(obj.bucket_name, obj.object_name)
        minio_connection.remove_bucket(bucket.name)

    assert not minio_connection.list_buckets()


@pytest.fixture()
def bucket(minio: Minio):
    request_id = 'test-request-' + (''.join(random.choices(string.ascii_lowercase + string.digits, k=10)))
    minio.make_bucket(request_id)
    assert not list(minio.list_objects_v2(request_id, recursive=True))

    yield request_id


@pytest.fixture()
def image_valid() -> Tuple[str, str]:
    yield 'library/alpine', 'latest'


@pytest.fixture()
def image_invalid() -> Tuple[str, str]:
    yield 'iterait/non-existing-image', 'latest'
