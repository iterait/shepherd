from typing import Dict

from apistrap.examples import ExamplesMixin, ModelExample
from schematics import Model
from schematics.types import StringType, BooleanType, DictType, ModelType

from .models import SheepModel, JobStatusModel


class ErrorResponse(Model):
    message: str = StringType(required=True)


class JobErrorResponse(ErrorResponse):
    pass


class StatusResponse(Model, ExamplesMixin):
    sheep: Dict[str, SheepModel] = DictType(ModelType(SheepModel), required=True)

    @classmethod
    def get_examples(cls):
        return [
            ModelExample("basic", cls({
                "sheep": {
                    "sheep_a": {
                        "running": True,
                        "model": {
                            "name": "OCR model",
                            "version": "1.0.42"
                        },
                        "request": "355d7806-daf2-4249-8581-63b5fcf0d335"
                    },
                    "sheep_b": {
                        "running": False,
                    }
                }
            }))
        ]


class StartJobResponse(Model):
    success: bool = BooleanType(default=True, required=True)


JobStatusResponse = JobStatusModel


class JobNotReadyResponse(Model):
    ready: bool = BooleanType(required=True, default=False)
