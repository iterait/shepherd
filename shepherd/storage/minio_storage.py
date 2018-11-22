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


_MINIO_FOLDER_DELIMITER = '/'
"""Minio folder delimiter."""


class MinioStorage(Storage):
    """
    A remote storage adapter that uses the S3 client from Minio SDK.
    """

    def __init__(self, minio: Minio):
        """
        Initialize the storage with a Minio handle.

        :param minio: Minio storage handle
        """
        self._minio = minio

    def is_accessible(self) -> bool:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.is_accessible`.
        """
        try:
            self._minio.list_buckets()
            return True
        except BaseException:
            return False

    def pull_job_data(self, job_id: str, target_directory: str) -> None:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.pull_job_data`.
        """
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

    def push_job_data(self, job_id: str, source_directory: str) -> None:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.push_job_data`.
        """
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

    def report_job_failed(self, job_id: str, message: str) -> None:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.report_job_failed`.
        """
        error = message.encode()
        try:
            self._minio.put_object(job_id, ERROR_FILE, BytesIO(error), len(error))
        except HTTPError as he:
            raise StorageInaccessibleError() from he
        except MinioError as me:
            raise StorageError(f"Failed to report job `{job_id}` as failed") from me

    def report_job_done(self, job_id: str) -> None:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.report_job_done`.
        """
        try:
            self._minio.put_object(job_id, DONE_FILE, BytesIO(b''), 0)
        except HTTPError as he:
            raise StorageInaccessibleError() from he
        except MinioError as me:
            raise StorageError(f"Failed to report job `{job_id}` as done") from me

    def is_job_done(self, job_id: str) -> bool:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.is_job_done`.
        """
        try:
            return minio_object_exists(self._minio, job_id, DONE_FILE) \
                   or minio_object_exists(self._minio, job_id, ERROR_FILE)
        except HTTPError as he:
            raise StorageInaccessibleError() from he
        except MinioError as me:
            raise StorageError(f"Failed to get status of job `{job_id}`") from me
