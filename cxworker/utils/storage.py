import os
import logging
import shutil
from os import path as path

from minio import Minio
from minio.error import MinioError

from cxworker.constants import INPUT_DIR, OUTPUT_DIR
from ..api.errors import StorageError

_MINIO_FOLDER_DELIMITER = '/'
"""Minio folder delimiter."""

def pull_minio_bucket(minio: Minio, bucket_name: str, dir_name: str) -> None:
    """
    Pull minio bucket contents to the specified directory.

    :param minio: Minio handle
    :param bucket_name: Minio bucket name
    :param dir_name: directory name to pull to
    :raise StorageError: if the pull fails
    """
    logging.debug('Pulling minio bucket `%s` to dir `%s`', bucket_name, dir_name)
    try:
        pulled_count = 0
        for object in minio.list_objects_v2(bucket_name, recursive=True):
            if object.object_name.startswith(INPUT_DIR + _MINIO_FOLDER_DELIMITER):
                filepath = path.join(*object.object_name.split(_MINIO_FOLDER_DELIMITER))
                os.makedirs(path.join(dir_name, path.dirname(filepath)), exist_ok=True)
                minio.fget_object(bucket_name, object.object_name, path.join(dir_name, filepath))
                pulled_count += 1
        if pulled_count == 0:
            logging.warning('No input objects pulled from bucket `%s`. Make sure they are in the `inputs/` folder.',
                            bucket_name)
    except MinioError as me:
        raise StorageError('Failed to pull minio bucket `{}`'.format(bucket_name)) from me


def push_minio_bucket(minio: Minio, bucket_name: str, dir_name: str) -> None:
    """
    Push directory contents to the specified minio bucket.

    :param minio: Minio handle
    :param bucket_name: Minio bucket name
    :param dir_name: directory name to push to from
    :raise StorageError: if the push fails
    """
    logging.debug('Pushing dir `%s` to minio bucket `%s`', dir_name, bucket_name)
    try:
        pushed_count = 0
        for prefix, _, files in os.walk(path.join(dir_name, OUTPUT_DIR)):
            for file in files:
                filepath = path.relpath(path.join(OUTPUT_DIR, prefix, file), dir_name)
                object_name = filepath.replace(path.sep, _MINIO_FOLDER_DELIMITER)
                minio.fput_object(bucket_name, object_name, path.join(dir_name, filepath))
                pushed_count += 1
        if pushed_count == 0:
            logging.warning('No output files pushed to bucket `%s`. Make sure they are in the `outputs/` folder.',
                            bucket_name)
    except MinioError as me:
        raise StorageError('Failed to push minio bucket `{}`'.format(bucket_name)) from me


def minio_object_exists(minio: Minio, bucket_name: str, object_name: str) -> bool:
    """
    Check if the specified minio object exists or not.

    :param minio: minio handle
    :param bucket_name: bucket name
    :param object_name: object name
    :return: true if the specified minio object exists, false otherwise
    """
    try:
        minio.stat_object(bucket_name, object_name)
        return True
    except MinioError:
        return False


def create_clean_dir(dir_path) -> str:
    """
    Create new directory (delete its contents if it exists).

    :param dir_path: directory path
    :return: path of the created directory
    """
    logging.debug('Creating clean dir dir `%s`', dir_path)
    if path.exists(dir_path):
        shutil.rmtree(dir_path)
    os.makedirs(dir_path)
    return dir_path
