from copy import deepcopy
from typing import Optional
from datetime import datetime

from apistrap.examples import ExamplesMixin, ModelExample
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


class JobStatusModel(Model, ExamplesMixin):
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

    @classmethod
    def get_examples(cls):
        return [
            ModelExample("finished", cls({
                "status": "done",
                "model": {
                    "name": "OCR model",
                    "version": "1.0.42"
                },
                "enqueued_at": datetime(2019, 1, 1, 12, 0),
                "processing_started_at": datetime(2019, 1, 1, 12, 10),
                "finished_at": datetime(2019, 1, 1, 12, 15)
            }), "A job that finished successfully"),
            ModelExample("pending", cls({
                "status": "queued",
                "model": {
                    "name": "OCR model",
                    "version": "1.0.42"
                },
                "enqueued_at": datetime(2019, 1, 1, 12, 0)
            }), "A job that waits to be processed"),
            ModelExample("failed", cls({
                "status": "failed",
                "model": {
                    "name": "OCR model",
                    "version": "1.0.42"
                },
                "error_details": {
                    "message": "An error occurred",
                    "exception_type": "ValueError",
                    "exception_traceback": """
                    file.py: 23
                    file_2.py: 47
                    """
                },
                "enqueued_at": datetime(2019, 1, 1, 12, 0),
                "processing_started_at": datetime(2019, 1, 1, 12, 10),
                "finished_at": datetime(2019, 1, 1, 12, 15)
            }), "A job that failed to be processed", "Exception details are only included if shepherd was launched in debug mode"),
        ]
