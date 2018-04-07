import pytest
from minio import Minio
import subprocess
import os
import random
import string
import os.path as path

from cxworker.utils import create_clean_dir

from cxworker.shepherd.config import RegistryConfig


@pytest.fixture()
def registry_config():
    yield RegistryConfig(dict(url='https://registry.hub.docker.com', username='cxworkertestdocker',
                              password='abc321321'))


@pytest.fixture(scope='session')
def minio():
    data_dir = path.join(create_clean_dir(path.join('/tmp', 'minio')))
    env = os.environ.copy()
    minio_key = 'AKIAIOSFODNN7EXAMPLE'
    minio_secret = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    env['MINIO_ACCESS_KEY'] = minio_key
    env['MINIO_SECRET_KEY'] = minio_secret
    minio_host = '0.0.0.0:7000'
    proc = subprocess.Popen(['minio', 'server', '--address', minio_host, data_dir], env=env)
    yield Minio(minio_host, access_key=minio_key, secret_key=minio_secret, secure=False)
    proc.kill()


@pytest.fixture()
def bucket(minio: Minio):
    request_id = 'test-request-' + (''.join(random.choices(string.ascii_lowercase + string.digits, k=10)))
    if minio.bucket_exists(request_id):
        for obj in minio.list_objects_v2(request_id, recursive=True):
            minio.remove_object(request_id, obj.object_name)
        minio.remove_bucket(request_id)
    minio.make_bucket(request_id)
    yield request_id
    if minio.bucket_exists(request_id):
        for obj in minio.list_objects_v2(request_id, recursive=True):
            minio.remove_object(request_id, obj.object_name)
        minio.remove_bucket(request_id)
