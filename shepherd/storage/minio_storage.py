from io import BytesIO

from minio import Minio

from shepherd.constants import DONE_FILE, ERROR_FILE
from shepherd.storage.storage import Storage
from shepherd.utils import minio_object_exists, pull_minio_bucket, push_minio_bucket


class MinioStorage(Storage):
    def __init__(self, minio: Minio):
        self._minio = minio

    def pull_job_data(self, job_id: str, target_directory: str):
        pull_minio_bucket(self._minio, job_id, target_directory)

    def push_job_data(self, job_id: str, source_directory: str):
        push_minio_bucket(self._minio, job_id, source_directory)

    def job_failed(self, job_id: str, message: str):
        error = message.encode()
        self._minio.put_object(job_id, ERROR_FILE, BytesIO(error), len(error))

    def job_done(self, job_id: str):
        self._minio.put_object(job_id, DONE_FILE, BytesIO(b''), 0)

    def is_job_done(self, job_id: str) -> bool:
        return minio_object_exists(self._minio, job_id, DONE_FILE) \
               or minio_object_exists(self._minio, job_id, ERROR_FILE)


