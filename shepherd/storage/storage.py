from io import BytesIO

import abc
import datetime
from typing import Optional


class Storage(metaclass=abc.ABCMeta):
    """
    An interface for services that provide access to job data in a remote storage.
    """

    @abc.abstractmethod
    def is_accessible(self) -> bool:
        """
        Check if the remote storage can be accessed.
        """

    @abc.abstractmethod
    def init_job(self, job_id: str) -> None:
        """
        Prepare storage for a new job.

        :param job_id: identifier of the new job
        """

    @abc.abstractmethod
    def job_data_exists(self, job_id: str) -> bool:
        """
        Check if the remote storage contains data for given job.

        :param job_id: identifier of the job to be checked
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    @abc.abstractmethod
    def pull_job_data(self, job_id: str, target_directory: str) -> None:
        """
        Download job files from the remote storage to a local directory.

        :param job_id: identifier of the job whose files should be downloaded
        :param target_directory: the directory where the files should be downloaded
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    @abc.abstractmethod
    def push_job_data(self, job_id: str, source_directory: str) -> None:
        """
        Upload processed job files from a local directory to the remote storage.

        :param job_id: identifier of the job whose files should be uploaded
        :param source_directory: the directory from which the files should be uploaded
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    @abc.abstractmethod
    def get_timestamp(self, job_id: str, file_path: str) -> datetime.datetime:
        """
        Get the timestamp of the last modification of given file.

        :param job_id: identifier of the job to which the file belongs
        :param file_path: path to the queried file
        :return: timestamp of last modification
        """

    @abc.abstractmethod
    def put_file(self, job_id: str, file_path: str, stream: BytesIO, length: int) -> None:
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
    def get_file(self, job_id: str, file_path: str) -> Optional[BytesIO]:
        """
        Download given file.

        :param job_id: identifier of the job to which the file belongs
        :param file_path: path to the queried file
        :return: a stream to read the file contents from
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    @abc.abstractmethod
    def report_job_failed(self, job_id: str, message: str) -> None:
        """
        Mark the job as failed in the remote storage.

        :param job_id: identifier of the failed job
        :param message: a description of the error
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    @abc.abstractmethod
    def report_job_done(self, job_id: str) -> None:
        """
        Mark the job as finished in the remote storage.

        :param job_id: identifier of the finished job
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    @abc.abstractmethod
    def is_job_done(self, job_id: str) -> bool:
        """
        Query the remote storage to find out if the job is done (either finished or failed).

        :param job_id: identifier of the queried job
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """
