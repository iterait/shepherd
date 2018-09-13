from schematics import Model
from schematics.exceptions import ValidationError
from schematics.types import StringType, ModelType

from shepherd.api.models import ModelModel


class StartJobRequest(Model):
    job_id: str = StringType(required=True)
    sheep_id: str = StringType(default=None)
    model: ModelModel = ModelType(ModelModel, required=True)
    payload: str = StringType(required=False)
