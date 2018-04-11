from schematics import Model
from schematics.exceptions import ValidationError
from schematics.types import StringType, ModelType

from cxworker.api.models import ModelModel


class StartJobRequest(Model):
    job_id: str = StringType(required=True)
    sheep_id: str = StringType(default=None)
    model: ModelModel = ModelType(ModelModel, required=True)
    payload_name: str = StringType(required=False)
    payload: str = StringType(required=False)

    def validate_payload(self, data, value):
        if data["payload"] is not None and data["payload_name"] is None:
            raise ValidationError("If a payload is supplied, you also need to provide payload_name")

        return value
