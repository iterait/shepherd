import os
import logging
import shutil
from os import path as path

from minio import Minio
from minio.error import MinioError
from urllib3.exceptions import HTTPError

from ..errors.api import StorageInaccessibleError


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
    except HTTPError as he:
        raise StorageInaccessibleError() from he
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
