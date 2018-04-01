import os
import logging
import shutil
from os import path as path

from minio import Minio
from minio.error import MinioError

from ..api.errors import StorageError

_MINIO_FOLDER_DELIMITER = '/'


def pull_minio_bucket(minio: Minio, bucket_name: str, dir_name: str) -> None:
    """
    Pull minio bucket contents to the specified directory.

    :param minio: Minio handle
    :param bucket_name: Minio bucket name
    :param dir_name: directory name to pull to
    :raise StorageError: if the pull fails
    """
    logging.debug('Pulling minio bucket `%s` to `%s`', bucket_name, dir_name)
    try:
        for object in minio.list_objects_v2(bucket_name, recursive=True):
            filepath = path.join(*object.object_name.split(_MINIO_FOLDER_DELIMITER))
            filedir = path.join(dir_name, path.dirname(filepath))
            os.makedirs(filedir, exist_ok=True)
            minio.fget_object(bucket_name, object.object_name, path.join(filedir, filepath))
    except MinioError as me:
        raise StorageError('Failed to pull minio bucket `{}`'.format(bucket_name)) from me


def push_minio_bucket(minio: Minio, bucket_name: str, dir_name: str) -> None:
    """
    Push directory contents to the specified minio bucket.

    :param minio: Minio handle
    :param bucket_name: Minio buckete name
    :param dir_name: directory name to push to from
    :raise StorageError: if the push fails
    """
    logging.debug('Pushing minio bucket `%s` to `%s`', bucket_name, dir_name)
    try:
        for prefix, _, files in os.walk(dir_name):
            for file in files:
                filepath = path.relpath(path.join(prefix, file), dir_name)
                object_name = filepath.replace(path.sep, _MINIO_FOLDER_DELIMITER)
                minio.fput_object(bucket_name, object_name, path.join(dir_name, filepath))
    except MinioError as me:
        raise StorageError('Failed to push minio bucket `{}`'.format(bucket_name)) from me


def create_clean_dir(dir_path) -> str:
    logging.debug('Creating clean dir dir `%s`', dir_path)
    if path.exists(dir_path):
        shutil.rmtree(dir_path)
    os.makedirs(dir_path)
    return dir_path
