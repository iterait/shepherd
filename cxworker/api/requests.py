from typing import NamedTuple


class StartJobRequest(NamedTuple):
    id: str
    container_id: str
    source_url: str
    result_url: str
    status_url: str
    refresh_model: bool = False


class InterruptJobRequest(NamedTuple):
    container_id: str


class ReconfigureRequest:
    def __init__(self, data):
        self.container_id = data["container_id"]
        self.model_name = data["model"]["name"]
        self.model_version = data["model"]["version"]

