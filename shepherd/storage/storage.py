import abc


class Storage(metaclass=abc.ABCMeta):
    """
    An interface for services that provide access to job data in a remote storage
    """

    @abc.abstractmethod
    def is_accessible(self) -> bool:
        """
        Check if the remote storage can be accessed
        """

    @abc.abstractmethod
    def pull_job_data(self, job_id: str, target_directory: str):
        """
        Download job files from the remote storage to a local directory
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    @abc.abstractmethod
    def push_job_data(self, job_id: str, source_directory: str):
        """
        Upload processed job files from a local directory to the remote storage
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    @abc.abstractmethod
    def job_failed(self, job_id: str, message: str):
        """
        Mark the job as failed in the remote storage
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    @abc.abstractmethod
    def job_done(self, job_id: str):
        """
        Mark the job as finished in the remote storage
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """

    @abc.abstractmethod
    def is_job_done(self, job_id: str) -> bool:
        """
        Query the remote storage to find out if the job is done (either finished or failed)
        :raises StorageInaccessibleError: the remote storage is not accessible
        :raises StorageError: there was an error when communicating with the remote storage
        """
