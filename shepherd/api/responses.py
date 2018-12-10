from typing import Dict
from datetime import datetime

from schematics import Model
from schematics.types import StringType, BooleanType, DictType, ModelType, DateTimeType

from .models import SheepModel, JobStatusModel


class ErrorResponse(Model):
    message: str = StringType(required=True)


class JobErrorResponse(ErrorResponse):
    pass


class StatusResponse(Model):
    containers: Dict[str, SheepModel] = DictType(ModelType(SheepModel), required=True)


class StartJobResponse(Model):
    success: bool = BooleanType(default=True, required=True)


JobStatusResponse = JobStatusModel


class JobReadyResponse(Model):
    ready: bool = BooleanType(required=True)
    finished_at: datetime = DateTimeType(required=False)
