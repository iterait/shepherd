import json
import os
import asyncio
import logging
from asyncio import StreamReader
from os import path
from io import BytesIO
from typing import Optional, BinaryIO, AsyncIterable
from xml.etree import ElementTree

from aiohttp.typedefs import LooseHeaders
import aiohttp
from aiohttp.client_exceptions import ClientError as AioHTTPClientError
from minio.helpers import get_target_url, get_md5_base64digest, get_sha256_hexdigest
from minio.signer import sign_v4

from .storage import Storage
from ..config import StorageConfig
from ..errors.api import StorageError, StorageInaccessibleError, NameConflictError, UnknownJobError
from ..constants import JOB_STATUS_FILE, INPUT_DIR, OUTPUT_DIR
from ..api.models import JobStatusModel


_MINIO_FOLDER_DELIMITER = '/'
"""Minio folder delimiter."""


class MinioStorage(Storage):
    """
    A remote storage adapter that uses the aiobotocore S3 client to access Minio.
    """

    _NS = {
        "s3": "http://s3.amazonaws.com/doc/2006-03-01/"
    }

    def __init__(self, storage_config: StorageConfig):
        """
        Initialize the storage according to the configuration.

        :param storage_config: storage configuration
        """

        self._session = aiohttp.ClientSession()
        self._config = storage_config

    @staticmethod
    def _ensure_user_agent_header(headers: Optional[LooseHeaders] = None) -> LooseHeaders:
        """Add User-Agent header if not specified yet."""
        if headers is None:
            headers = {}

        headers.setdefault("User-Agent", "Shepherd")

        return headers

    def _ensure_auth_headers(self, method: str, url: str, headers: Optional[LooseHeaders] = None,
                             content_sha256: Optional[str] = None) -> LooseHeaders:
        """
        Make sure that given header set contains all headers required for a successful authentication.
        """

        headers = self._ensure_user_agent_header(headers)

        return sign_v4(method.upper(), url, "us-east-1", headers, self._config.access_key, self._config.secret_key,
                       content_sha256=content_sha256)

    async def init_job(self, job_id: str):
        """
        Implementation of :py:meth:`shepherd.storage.Storage.init_job`.
        """

        url = get_target_url(self._config.url, bucket_name=job_id)
        headers = self._ensure_auth_headers("PUT", url)

        try:
            response = await self._session.put(url, headers=headers)
        except AioHTTPClientError as he:
            raise StorageInaccessibleError() from he

        if response.status == 409:
            raise NameConflictError("A job with this ID was already submitted")

        if response.status != 200:
            raise StorageError(f"Failed to create minio bucket `{job_id}`")

    async def is_accessible(self) -> bool:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.is_accessible`.
        """

        url = get_target_url(self._config.url, "does-not-matter")
        headers = self._ensure_auth_headers('HEAD', url)

        try:
            await self._session.head(url, headers=headers)
            return True
        except AioHTTPClientError:
            return False

    async def job_dir_exists(self, job_id: str) -> bool:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.job_data_exists`.
        """

        url = get_target_url(self._config.url, job_id)
        headers = self._ensure_auth_headers('HEAD', url)

        try:
            response = await self._session.head(url, headers=headers)
        except AioHTTPClientError as error:
            raise StorageInaccessibleError(f"Failed to check minio bucket `{job_id}`") from error

        return response.status == 200

    async def _list_bucket(self, bucket: str) -> AsyncIterable[str]:
        """
        List the names of all files in a bucket.

        :param bucket: the bucket to list
        :return: a generator of file names
        """

        continuation_token = None
        truncated = True

        while truncated:
            query = {}
            if continuation_token is not None:
                query["continuation-token"] = continuation_token

            url = get_target_url(self._config.url, bucket_name=bucket, query=query)
            headers = self._ensure_auth_headers("GET", url)

            try:
                response = await self._session.get(url, headers=headers)
            except AioHTTPClientError as ce:
                raise StorageInaccessibleError() from ce

            if response.status != 200:
                raise StorageError(f"Listing minio bucket `{bucket}` failed")

            tree = ElementTree.fromstring(await response.text())

            for key in map(lambda el: el.text, tree.findall(".//s3:Key", self._NS)):
                yield key

            truncated = tree.find("s3:IsTruncated", self._NS).text != "false"
            continuation_token = tree.find("s3:NextContinuationToken", self._NS)

    async def _get_object(self, bucket: str, object_name: str, destination: BinaryIO) -> None:
        """
        Fetch a remote object into a binary file/stream.

        :param bucket: the bucket where the object is stored
        :param object_name: the path to the object
        """

        url = get_target_url(self._config.url, bucket_name=bucket, object_name=object_name)
        headers = self._ensure_auth_headers("GET", url)

        try:
            async with self._session.get(url, headers=headers) as response:
                if response.status != 200:
                    raise StorageError(f"Could not fetch `{bucket}/{object_name}` from minio")

                while True:
                    chunk = await response.content.read(128 * 1024)

                    if not chunk:
                        break

                    destination.write(chunk)
        except AioHTTPClientError as ce:
            raise StorageInaccessibleError() from ce

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

        if not await self.job_dir_exists(job_id):
            raise StorageError(f"Job directory for `{job_id}` does not exist")

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

    async def _put_object(self, bucket: str, object_name: str, content: BinaryIO, length: int) -> None:
        """
        Store data from a file/stream object as a remote object.

        :param bucket: the bucket where the object should be stored
        :param object_name: the name of the new object
        :param content: a stream containing the object data
        :param length: the length of the data
        """

        url = get_target_url(self._config.url, bucket_name=bucket, object_name=object_name)

        headers = self._ensure_user_agent_header({
            "Content-Length": str(length),
            "Content-Type": "application/octet-stream"
        })

        data = content.read()
        content_sha256 = get_sha256_hexdigest(data)

        if self._config.secure:
            headers["Content-Md5"] = get_md5_base64digest(data)
            content_sha256 = "UNSIGNED-PAYLOAD"

        headers = self._ensure_auth_headers("PUT", url, headers, content_sha256=content_sha256)

        try:
            response = await self._session.put(url, data=data, headers=headers)
        except AioHTTPClientError as ce:
            raise StorageInaccessibleError() from ce

        if response.status != 200:
            raise StorageError(f"Failed to upload object `{bucket}/{object_name}`")

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

        if not await self.job_dir_exists(job_id):
            raise StorageError(f"Job directory for `{job_id}` does not exist")

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

    async def put_file(self, job_id: str, file_path: str, stream: BinaryIO, length: int) -> None:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.put_file`.
        """
        await self._put_object(job_id, file_path, stream, length)

    async def _object_exists(self, bucket: str, object_name: str) -> bool:
        """
        Check if an object exists in the remote storage.

        :param bucket: the bucket to search for the object
        :param object_name: name of the object
        :return: True if the object exists, False otherwise
        """

        url = get_target_url(self._config.url, bucket_name=bucket, object_name=object_name)
        headers = self._ensure_auth_headers("HEAD", url)

        try:
            response = await self._session.head(url, headers=headers)

            return response.status == 200
        except AioHTTPClientError as ce:
            raise StorageInaccessibleError() from ce

    async def get_file(self, job_id: str, file_path: str) -> Optional[StreamReader]:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.get_file`.
        """

        url = get_target_url(self._config.url, bucket_name=job_id, object_name=file_path)
        headers = self._ensure_auth_headers("GET", url)

        try:
            if not await self._object_exists(job_id, file_path):
                return None

            response = await self._session.get(url, headers=headers)

            return response.content
        except AioHTTPClientError as he:
            raise StorageInaccessibleError() from he

    async def set_job_status(self, job_id: str, status: JobStatusModel) -> None:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.set_job_status`
        """
        data = BytesIO(json.dumps(status.to_primitive()).encode())

        length = len(data.read())
        data.seek(0)

        try:
            await self._put_object(job_id, JOB_STATUS_FILE, data, length)
        except StorageError as ce:
            raise StorageError(f"Failed to update status of job `{job_id}`") from ce

    async def get_job_status(self, job_id: str) -> JobStatusModel:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.get_job_status`.
        """
        try:
            if not await self.job_dir_exists(job_id) or not await self._object_exists(job_id, JOB_STATUS_FILE):
                raise UnknownJobError('Data for job `{}` does not exist'.format(job_id))

            data = BytesIO()
            await self._get_object(job_id, JOB_STATUS_FILE, data)
        except AioHTTPClientError as he:
            raise StorageInaccessibleError() from he
        except StorageError as ce:
            raise StorageError(f"Failed to get status of job `{job_id}`") from ce

        data.seek(0)
        return JobStatusModel(json.load(data))

    async def close(self) -> None:
        """
        Implementation of :py:meth:`shepherd.storage.Storage.close`.
        """

        try:
            await self._session.close()
        except AioHTTPClientError as he:
            raise StorageError("There was an error while closing the AioHTTP client session") from he
