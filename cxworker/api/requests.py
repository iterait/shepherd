from collections import namedtuple

StartJobRequest = namedtuple("StartJobRequest", ("id", "container_id", "source_url", "result_url", "status_url"))

InterruptJobRequest = namedtuple("InterruptJobRequest", ("container_id", ))


class ReconfigureRequest:
    def __init__(self, data):
        self.container_id = data["container_id"]
        self.model_name = data["model"]["name"]
        self.model_version = data["model"]["version"]

