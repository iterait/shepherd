import aiobotocore
import asyncio
import json
import os
from aiohttp.client_exceptions import ClientError as AioHTTPClientError
from botocore.exceptions import ClientError as BotocoreClientError

from os import path

import logging
from io import BytesIO

from typing import Optional, BinaryIO, AsyncIterable

from ..config import StorageConfig
from ..errors.api import StorageError, StorageInaccessibleError, NameConflictError, UnknownJobError
from ..constants import JOB_STATUS_FILE, INPUT_DIR, OUTPUT_DIR
from .storage import Storage
from ..api.models import JobStatusModel


_MINIO_FOLDER_DELIMITER = '/'
"""Minio folder delimiter."""


def _botocore_code(error: BotocoreClientError) -> str:
    """
    Extract an error code from a botocore exception.

    :param error: the error to extract
    :return: the error code (sometimes a human-readable string, sometimes a number in string form)
    """
    return error.response.get("Error", {}).get("Code", "")


class MinioStorage(Storage):
    """
    A remote storage adapter that uses the aiobotocore S3 client to access Minio.
    """

    def __init__(self, storage_config: StorageConfig):
        """
        Initialize the storage according to the configuration.

        :param storage_config: storage configuration
        """

        session = aiobotocore.get_session(loop=asyncio.get_event_loop())
        self._client = session.create_client('s3',
                                             endpoint_url=storage_config.url,
                                             use_ssl=storage_config.secure,
                                             aws_access_key_id=storage_config.access_key,
                                             aws_secret_access_key=storage_config.secret_key)

    async def init_job(self, job_id: str):
        """
        Implementation of :py:meth:`shepherd.storage.Storage.init_job`.
        """

        try:
            await self._client.create_bucket(Bucket=job_id)
        except AioHTTPClientError as he:
            raise StorageInaccessibleError() from he
        except BotocoreClientError as ce:
            code = _botocore_code(ce)
            if code == "BucketAlreadyExists" or code == "BucketAlreadyOwnedByYou":
                raise NameConflictError("A job with this ID was already submitted") from ce
            raise

    async def is_accessible(self) -> bool:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.is_accessible`.
        """
        try:
            await self._client.list_buckets()
            return True
        except BaseException:
            return False

    async def job_dir_exists(self, job_id: str) -> bool:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.job_data_exists`.
        """
        try:
            await self._client.head_bucket(Bucket=job_id)
            return True
        except AioHTTPClientError as he:
            raise StorageInaccessibleError() from he
        except BotocoreClientError as ce:
            if _botocore_code(ce) == "404":
                return False

            raise StorageError('Failed to check minio bucket `{}`'.format(job_id)) from ce

    async def _list_bucket(self, bucket: str) -> AsyncIterable[str]:
        """
        List the names of all files in a bucket.

        :param bucket: the bucket to list
        :return: a generator of file names
        """

        continuation_token = None
        truncated = True

        while truncated:
            if continuation_token is None:
                response = await self._client.list_objects_v2(Bucket=bucket)
            else:
                response = await self._client.list_objects_v2(Bucket=bucket, ContinuationToken=continuation_token)

            truncated = response.get('IsTruncated', False)
            continuation_token = response.get('NextContinuationToken')

            for obj in response.get('Contents', []):
                yield obj['Key']

    async def _get_object(self, bucket: str, object_name: str, destination: BinaryIO) -> None:
        """
        Fetch a remote object into a binary file/stream.

        :param bucket: the bucket where the object is stored
        :param object_name: the path to the object
        """
        response = await self._client.get_object(Bucket=bucket, Key=object_name)

        async with response["Body"] as source:
            while True:
                chunk = await source.read(16384)

                if chunk == b"":
                    break

                destination.write(chunk)

    async def _download_object(self, bucket: str, object_name: str, destination_path: str) -> None:
        """
        Download a remote object into a file identified by a path.

        :param bucket: the bucket where the object is stored
        :param object_name: the path to the object
        :param destination_path: where the object should be stored
        """

        with open(destination_path, "wb") as destination:
            await self._get_object(bucket, object_name, destination)

    async def pull_job_data(self, job_id: str, target_directory: str) -> None:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.pull_job_data`.
        """
        logging.debug('Pulling minio bucket `%s` to dir `%s`', job_id, target_directory)

        try:
            # Make sure the bucket exists
            await self._client.head_bucket(Bucket=job_id)

            pulled_count = 0
            tasks = []

            async for file_name in self._list_bucket(job_id):
                if file_name.startswith(INPUT_DIR + _MINIO_FOLDER_DELIMITER):
                    filepath = path.join(*file_name.split(_MINIO_FOLDER_DELIMITER))
                    os.makedirs(path.join(target_directory, path.dirname(filepath)), exist_ok=True)
                    tasks.append(self._download_object(job_id, file_name, path.join(target_directory, filepath)))
                    pulled_count += 1

            await asyncio.gather(*tasks)

            if pulled_count == 0:
                logging.warning('No input objects pulled from bucket `%s`. Make sure they are in the `inputs/` folder.',
                                job_id)
        except AioHTTPClientError as he:
            raise StorageInaccessibleError() from he
        except BotocoreClientError as ce:
            raise StorageError('Failed to pull minio bucket `{}`'.format(job_id)) from ce

    async def _put_object(self, bucket: str, object_name: str, content: BinaryIO, length: int) -> None:
        """
        Store data from a file/stream object as a remote object.

        :param bucket: the bucket where the object should be stored
        :param object_name: the name of the new object
        :param content: a stream containing the object data
        :param length: the length of the data
        """
        # for some reason, put_object hangs indefinitely when using an unknown bucket instead of throwing
        #   => we check whether the bucket exists beforehand
        await self._client.head_bucket(Bucket=bucket)
        await self._client.put_object(Bucket=bucket, Key=object_name, Body=content, ContentLength=length)

    async def _upload_object(self, bucket: str, object_name: str, source_path: str):
        """
        Store the contents of a file identified by a path in a remote object.

        :param bucket: the bucket where the object should stored
        :param object_name: the name of the new object
        :param source_path: the path of the source file
        """
        with open(source_path, 'rb') as source:
            await self._put_object(bucket, object_name, source, os.stat(source_path).st_size)

    async def push_job_data(self, job_id: str, source_directory: str) -> None:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.push_job_data`.
        """
        logging.debug('Pushing dir `%s` to minio bucket `%s`', source_directory, job_id)
        try:
            # Make sure the bucket exists
            await self._client.head_bucket(Bucket=job_id)

            pushed_count = 0
            tasks = []

            for prefix, _, files in os.walk(path.join(source_directory, OUTPUT_DIR)):
                for file in files:
                    filepath = path.relpath(path.join(OUTPUT_DIR, prefix, file), source_directory)
                    object_name = filepath.replace(path.sep, _MINIO_FOLDER_DELIMITER)
                    source_path = path.join(source_directory, filepath)
                    tasks.append(self._upload_object(job_id, object_name, source_path))
                    pushed_count += 1

            await asyncio.gather(*tasks)

            if pushed_count == 0:
                logging.warning('No output files pushed to bucket `%s`. Make sure they are in the `outputs/` folder.',
                                job_id)
        except AioHTTPClientError as he:
            raise StorageInaccessibleError() from he
        except BotocoreClientError as ce:
            raise StorageError('Failed to push minio bucket `{}`'.format(job_id)) from ce

    async def put_file(self, job_id: str, file_path: str, stream: BinaryIO, length: int) -> None:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.put_file`.
        """
        try:
            await self._put_object(job_id, file_path, stream, length)
        except AioHTTPClientError as he:
            raise StorageInaccessibleError() from he
        except BotocoreClientError as ce:
            raise StorageError(f"Failed to save file `{file_path}` for job `{job_id}`") from ce

    async def _object_exists(self, bucket: str, object_name: str) -> bool:
        """
        Check if an object exists in the remote storage.

        :param bucket: the bucket to search for the object
        :param object_name: name of the object
        :return: True if the object exists, False otherwise
        """
        try:
            await self._client.head_object(Bucket=bucket, Key=object_name)
            return True
        except AioHTTPClientError as he:
            raise StorageInaccessibleError() from he
        except BotocoreClientError as ce:
            if _botocore_code(ce) == "404":
                return False

            raise

    async def get_file(self, job_id: str, file_path: str) -> Optional[BinaryIO]:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.get_file`.
        """
        try:
            if not await self._object_exists(job_id, file_path):
                return None

            response = await self._client.get_object(Bucket=job_id, Key=file_path)
            async with response["Body"] as stream:
                return BytesIO(await stream.read())  # TODO this could be redone without storing the file in memory
        except AioHTTPClientError as he:
            raise StorageInaccessibleError() from he
        except BotocoreClientError as ce:
            raise StorageError(f"Failed to get file `{file_path}` from job `{job_id}`") from ce

    async def set_job_status(self, job_id: str, status: JobStatusModel) -> None:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.set_job_status`
        """
        data = BytesIO(json.dumps(status.to_primitive()).encode())

        length = len(data.read())
        data.seek(0)

        try:
            await self._put_object(job_id, JOB_STATUS_FILE, data, length)
        except AioHTTPClientError as he:
            raise StorageInaccessibleError() from he
        except BotocoreClientError as ce:
            raise StorageError(f"Failed to update status of job `{job_id}`") from ce

    async def get_job_status(self, job_id: str) -> JobStatusModel:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.get_job_status`.
        """
        try:
            if not await self.job_dir_exists(job_id) or not await self._object_exists(job_id, JOB_STATUS_FILE):
                raise UnknownJobError('Data for job `{}` does not exist'.format(job_id))

            response = await self._client.get_object(Bucket=job_id, Key=JOB_STATUS_FILE)
        except AioHTTPClientError as he:
            raise StorageInaccessibleError() from he
        except BotocoreClientError as ce:
            raise StorageError(f"Failed to get status of job `{job_id}`") from ce

        async with response["Body"] as stream:
            return JobStatusModel(json.loads(await stream.read()))
