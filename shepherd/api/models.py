from copy import deepcopy
from typing import Optional
from datetime import datetime

from schematics import Model
from schematics.types import StringType, BooleanType, ModelType, UUIDType, DateTimeType


class ModelModel(Model):
    """
    Information about the model used for a job.
    """
    name: Optional[str] = StringType(serialize_when_none=True)
    version: Optional[str] = StringType(serialize_when_none=True)


class SheepModel(Model):
    """
    Information about a sheep.
    """
    running: bool = BooleanType(required=True)
    model: ModelModel = ModelType(ModelModel, required=True)
    request: Optional[str] = UUIDType(serialize_when_none=True)


class ErrorModel(Model):
    """
    Information about an error that occurred when processing a job.
    """
    message: str = StringType(required=True)
    exception_type: str = StringType(required=False)
    exception_traceback: str = StringType(required=False)


class JobStatus:
    """
    Used as an enum class that represents all possible states of a job.
    """
    ACCEPTED = "accepted"
    QUEUED = "queued"
    PROCESSING = "processing"
    FAILED = "failed"
    DONE = "done"


class JobStatusModel(Model):
    """
    Status information for a job.
    """
    status: JobStatus = StringType(required=True, choices=[*map(
        lambda m: getattr(JobStatus, m),
        filter(str.isupper, dir(JobStatus)))
    ])
    error_details: ErrorModel = ModelType(ErrorModel, required=False, default=None)
    model: ModelModel = ModelType(ModelModel, required=True)
    enqueued_at: datetime = DateTimeType(required=False)
    processing_started_at: datetime = DateTimeType(required=False)
    finished_at: datetime = DateTimeType(required=False)

    def copy(self) -> 'JobStatusModel':
        """
        Make a deep copy of this object.

        :return: a deep copy of this object
        """
        return JobStatusModel(deepcopy(self.to_primitive()))
