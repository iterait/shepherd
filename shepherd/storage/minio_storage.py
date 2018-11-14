from io import BytesIO

from minio import Minio

from shepherd.constants import DONE_FILE, ERROR_FILE
from shepherd.storage.storage import Storage
from shepherd.utils import minio_object_exists, pull_minio_bucket, push_minio_bucket


class MinioStorage(Storage):
    """
    A remote storage adapter that uses the S3 client from Minio SDK.
    """

    def __init__(self, minio: Minio):
        self._minio = minio

    def is_accessible(self) -> bool:
        try:
            self._minio.list_buckets()
            return True
        except BaseException:
            return False

    is_accessible.__doc__ = Storage.is_accessible.__doc__

    def pull_job_data(self, job_id: str, target_directory: str) -> None:
        pull_minio_bucket(self._minio, job_id, target_directory)

    pull_job_data.__doc__ = Storage.pull_job_data.__doc__

    def push_job_data(self, job_id: str, source_directory: str) -> None:
        push_minio_bucket(self._minio, job_id, source_directory)

    push_job_data.__doc__ = Storage.push_job_data.__doc__

    def report_job_failed(self, job_id: str, message: str) -> None:
        error = message.encode()
        self._minio.put_object(job_id, ERROR_FILE, BytesIO(error), len(error))

    report_job_failed.__doc__ = Storage.report_job_failed.__doc__

    def report_job_done(self, job_id: str) -> None:
        self._minio.put_object(job_id, DONE_FILE, BytesIO(b''), 0)

    report_job_done.__doc__ = Storage.report_job_done.__doc__

    def is_job_done(self, job_id: str) -> bool:
        return minio_object_exists(self._minio, job_id, DONE_FILE) \
               or minio_object_exists(self._minio, job_id, ERROR_FILE)

    is_job_done.__doc__ = Storage.is_job_done.__doc__
