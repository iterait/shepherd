from typing import Dict

from schematics import Model
from schematics.types import StringType, BooleanType, DictType, ModelType

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


class JobNotReadyResponse(Model):
    ready: bool = BooleanType(required=True, default=False)
