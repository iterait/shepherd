from typing import Optional, List
from schematics import Model
from schematics.types import StringType, URLType, BooleanType, ListType, ModelType

from cxworker.api.models import ModelModel


class StartJobRequest(Model):
    id: str = StringType(required=True)
    container_id: str = StringType(required=True)
    source_url: str = StringType(required=True)
    result_url: str = StringType(required=True)
    status_url: Optional[str] = StringType(default=None, serialize_when_none=True)
    refresh_model: bool = BooleanType(default=False)


class InterruptJobRequest(Model):
    container_id: str = StringType(required=True)


class ReconfigureRequest(Model):
    model: ModelModel = ModelType(ModelModel, required=True)
    container_id: str = StringType(required=True)
    slave_container_ids: List[str] = ListType(StringType, default=lambda: [])
