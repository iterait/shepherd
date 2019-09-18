from typing import Dict

from apistrap.schemas import ErrorResponse
from schematics import Model
from schematics.types import BooleanType, DictType, ModelType

from .models import SheepModel, JobStatusModel


class JobErrorResponse(ErrorResponse):
    pass


class StatusResponse(Model):
    containers: Dict[str, SheepModel] = DictType(ModelType(SheepModel), required=True)


class StartJobResponse(Model):
    success: bool = BooleanType(default=True, required=True)


JobStatusResponse = JobStatusModel


class JobNotReadyResponse(Model):
    ready: bool = BooleanType(required=True, default=False)
