from typing import Dict

from schematics import Model
from schematics.types import StringType, BooleanType, DictType, ModelType

from shepherd.api.models import SheepModel


class ErrorResponse(Model):
    message: str = StringType(required=True)


class JobErrorResponse(ErrorResponse):
    pass


class StatusResponse(Model):
    containers: Dict[str, SheepModel] = DictType(ModelType(SheepModel), required=True)


class StartJobResponse(Model):
    success: bool = BooleanType(default=True, required=True)


class JobStatusResponse(Model):
    ready: bool = BooleanType(required=True)
