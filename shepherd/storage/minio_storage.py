import os

from os import path

import logging
from io import BytesIO

from minio import Minio
from minio.error import MinioError
from urllib3.exceptions import HTTPError

from ..errors.api import StorageError, StorageInaccessibleError
from ..constants import DONE_FILE, ERROR_FILE, INPUT_DIR, OUTPUT_DIR
from .storage import Storage
from ..utils import minio_object_exists


def inherit_doc(parent):
    """
    A decorator that declares a method should inherit its doc block from its ancestor
    :param parent: The parent class whose doc block should be used
    """
    def decorator(f):
        f.__doc__ = getattr(parent, f.__name__).__doc__
        return f

    return decorator


_MINIO_FOLDER_DELIMITER = '/'
"""Minio folder delimiter."""


class MinioStorage(Storage):
    """
    A remote storage adapter that uses the S3 client from Minio SDK.
    """

    def __init__(self, minio: Minio):
        self._minio = minio

    @inherit_doc(Storage)
    def is_accessible(self) -> bool:
        try:
            self._minio.list_buckets()
            return True
        except BaseException:
            return False

    @inherit_doc(Storage)
    def pull_job_data(self, job_id: str, target_directory: str) -> None:
        logging.debug('Pulling minio bucket `%s` to dir `%s`', job_id, target_directory)

        try:
            pulled_count = 0
            for object in self._minio.list_objects_v2(job_id, recursive=True):
                if object.object_name.startswith(INPUT_DIR + _MINIO_FOLDER_DELIMITER):
                    filepath = path.join(*object.object_name.split(_MINIO_FOLDER_DELIMITER))
                    os.makedirs(path.join(target_directory, path.dirname(filepath)), exist_ok=True)
                    self._minio.fget_object(job_id, object.object_name, path.join(target_directory, filepath))
                    pulled_count += 1
            if pulled_count == 0:
                logging.warning('No input objects pulled from bucket `%s`. Make sure they are in the `inputs/` folder.',
                                job_id)
        except HTTPError as he:
            raise StorageInaccessibleError() from he
        except MinioError as me:
            raise StorageError('Failed to pull minio bucket `{}`'.format(job_id)) from me

    @inherit_doc(Storage)
    def push_job_data(self, job_id: str, source_directory: str) -> None:
        logging.debug('Pushing dir `%s` to minio bucket `%s`', source_directory, job_id)
        try:
            pushed_count = 0
            for prefix, _, files in os.walk(path.join(source_directory, OUTPUT_DIR)):
                for file in files:
                    filepath = path.relpath(path.join(OUTPUT_DIR, prefix, file), source_directory)
                    object_name = filepath.replace(path.sep, _MINIO_FOLDER_DELIMITER)
                    self._minio.fput_object(job_id, object_name, path.join(source_directory, filepath))
                    pushed_count += 1
            if pushed_count == 0:
                logging.warning('No output files pushed to bucket `%s`. Make sure they are in the `outputs/` folder.',
                                job_id)
        except HTTPError as he:
            raise StorageInaccessibleError() from he
        except MinioError as me:
            raise StorageError('Failed to push minio bucket `{}`'.format(job_id)) from me

    @inherit_doc(Storage)
    def report_job_failed(self, job_id: str, message: str) -> None:
        error = message.encode()
        try:
            self._minio.put_object(job_id, ERROR_FILE, BytesIO(error), len(error))
        except HTTPError as he:
            raise StorageInaccessibleError() from he
        except MinioError as me:
            raise StorageError(f"Failed to report job `{job_id}` as failed") from me

    @inherit_doc(Storage)
    def report_job_done(self, job_id: str) -> None:
        try:
            self._minio.put_object(job_id, DONE_FILE, BytesIO(b''), 0)
        except HTTPError as he:
            raise StorageInaccessibleError() from he
        except MinioError as me:
            raise StorageError(f"Failed to report job `{job_id}` as done") from me

    @inherit_doc(Storage)
    def is_job_done(self, job_id: str) -> bool:
        try:
            return minio_object_exists(self._minio, job_id, DONE_FILE) \
                   or minio_object_exists(self._minio, job_id, ERROR_FILE)
        except HTTPError as he:
            raise StorageInaccessibleError() from he
        except MinioError as me:
            raise StorageError(f"Failed to get status of job `{job_id}`") from me
