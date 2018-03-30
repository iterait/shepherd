from typing import Optional, List
from schematics import Model
from schematics.types import StringType, URLType, BooleanType, ListType, ModelType

from cxworker.api.models import ModelModel


class StartJobRequest(Model):
    job_id: str = StringType(required=True)
    sheep_id: str = StringType(default=None)
    model: ModelModel = ModelType(ModelModel, required=True)

