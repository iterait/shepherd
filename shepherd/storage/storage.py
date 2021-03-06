import abc
from asyncio import StreamReader
from typing import Optional, BinaryIO

from ..api.models import JobStatusModel


class Storage(metaclass=abc.ABCMeta):
    """
    An interface for services that provide access to job data in a remote storage.
    """

    @abc.abstractmethod
    async def is_accessible(self) -> bool:
        """
        Check if the remote storage can be accessed.
        """

    @abc.abstractmethod
    async def init_job(self, job_id: str) -> None:
        """
        Prepare storage for a new job.

        :param job_id: identifier of the new job
        """

    @abc.abstractmethod
    async def job_dir_exists(self, job_id: str) -> bool:
        """
        Check if the remote storage contains directory/bucket for the given job.

        :param job_id: identifier of the job to be checked
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    @abc.abstractmethod
    async def pull_job_data(self, job_id: str, target_directory: str) -> None:
        """
        Download job files from the remote storage to a local directory.

        :param job_id: identifier of the job whose files should be downloaded
        :param target_directory: the directory where the files should be downloaded
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    @abc.abstractmethod
    async def push_job_data(self, job_id: str, source_directory: str) -> None:
        """
        Upload processed job files from a local directory to the remote storage.

        :param job_id: identifier of the job whose files should be uploaded
        :param source_directory: the directory from which the files should be uploaded
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    @abc.abstractmethod
    async def put_file(self, job_id: str, file_path: str, stream: BinaryIO, length: int) -> None:
        """
        Store given file.

        :param job_id: identifier of the job to which the file belongs
        :param file_path: path to the stored file
        :param stream: stream to read file contents from
        :param length: length of the content stream
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    @abc.abstractmethod
    async def get_file(self, job_id: str, file_path: str) -> Optional[StreamReader]:
        """
        Download given file.

        :param job_id: identifier of the job to which the file belongs
        :param file_path: path to the queried file
        :return: a stream to read the file contents from
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    async def set_job_status(self, job_id: str, status: JobStatusModel) -> None:
        """
        Update the status file for a job.

        :param job_id: identifier of the updated job
        :param status: new status information
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    @abc.abstractmethod
    async def get_job_status(self, job_id: str) -> JobStatusModel:
        """
        Query the remote storage to find out if the job is done (either finished or failed).
        This should not throw an error if the job does not exist (yet).

        :param job_id: identifier of the queried job
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        :raises UnknownJobError: status for an unknown job was requested
        """

    async def close(self) -> None:
        """
        Perform cleanup tasks (if necessary - e.g. terminate connection pools).

        :raises StorageError: an error occurred while the storage was being closed
        """
