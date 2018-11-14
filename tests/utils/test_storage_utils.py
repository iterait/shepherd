import pytest
import os
import os.path as path
import io

from minio import Minio

from shepherd.constants import INPUT_DIR, OUTPUT_DIR
from shepherd.utils import *
from shepherd.errors.api import StorageError


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


def test_minio_push(minio: Minio, bucket, tmpdir, caplog):
    job_dir = path.join(tmpdir, bucket)
    create_clean_dir(job_dir)
    inputs_dir = create_clean_dir(path.join(job_dir, OUTPUT_DIR))
    another_dir = create_clean_dir(path.join(job_dir, 'another'))

    # test warning
    push_minio_bucket(minio, bucket, job_dir)
    assert 'No output files pushed to bucket' in caplog.text

    # create two files
    for dir_ in (inputs_dir, another_dir):
        with open(path.join(inputs_dir, 'file.txt'), 'w') as file:
            file.write(dir_+'content')

    # test if only the one in the outputs folder is pushed
    push_minio_bucket(minio, bucket, job_dir)
    minio_objects = list(minio.list_objects_v2(bucket, recursive=True))
    assert len(minio_objects) == 1
    assert minio_objects[0].object_name == OUTPUT_DIR + '/file.txt'

    with pytest.raises(StorageError):
        push_minio_bucket(minio, bucket+'-missing', job_dir)

    assert minio_object_exists(minio, bucket, OUTPUT_DIR + '/file.txt')
    assert not minio_object_exists(minio, bucket, 'another/file.txt')


def test_minio_pull(minio: Minio, bucket, tmpdir, caplog):
    job_dir = path.join(tmpdir, bucket)
    create_clean_dir(job_dir)
    pull_minio_bucket(minio, bucket, job_dir)
    assert 'No input objects pulled from bucket' in caplog.text
    data = b'some data'
    minio.put_object(bucket, INPUT_DIR + '/file.dat', io.BytesIO(data), len(data))
    minio.put_object(bucket, 'another/file.dat', io.BytesIO(data), len(data))

    assert minio_object_exists(minio, bucket, INPUT_DIR + '/file.dat')
    assert minio_object_exists(minio, bucket, 'another/file.dat')

    pull_minio_bucket(minio, bucket, job_dir)
    filepath = path.join(job_dir, INPUT_DIR, 'file.dat')
    assert path.exists(filepath)
    with open(filepath) as file:
        assert file.read() == 'some data'
    assert not path.exists(path.join(job_dir, 'another', 'file.dat'))

    with pytest.raises(StorageError):
        pull_minio_bucket(minio, bucket+'-missing', job_dir)
