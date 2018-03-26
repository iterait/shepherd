from typing import Optional

from schematics import Model
from schematics.types import StringType, BooleanType, ModelType, UUIDType


class ModelModel(Model):
    name: Optional[str] = StringType(serialize_when_none=True)
    version: Optional[str] = StringType(serialize_when_none=True)


class ContainerModel(Model):
    running: bool = BooleanType(required=True)
    model: ModelModel = ModelType(ModelModel, required=True)
    request: Optional[str] = UUIDType(serialize_when_none=True)
