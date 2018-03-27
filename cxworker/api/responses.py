from typing import Dict

from schematics import Model
from schematics.types import StringType, BooleanType, DictType, ModelType

from cxworker.api.models import SheepModel


class ErrorResponse(Model):
    message: str = StringType(required=True)


class StatusResponse(Model):
    containers: Dict[str, SheepModel] = DictType(ModelType(SheepModel), required=True)


class InterruptJobResponse(Model):
    success: bool = BooleanType(default=True, required=True)


class StartJobResponse(Model):
    success: bool = BooleanType(default=True, required=True)


class ReconfigureResponse(Model):
    success: bool = BooleanType(default=True, required=True)
