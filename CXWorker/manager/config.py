import yaml
from typing import Mapping, List


class ContainerConfig:
    def __init__(self, port: int, type: str, devices: List[str]):
        self.port = port
        self.type = type
        self.devices = devices


class WorkerConfig:
    def __init__(self, registry: str, containers: Mapping[str, ContainerConfig]):
        self.registry = registry
        self.containers = containers


def load_config(config_stream):
    config_object = yaml.load(config_stream)
    containers = {}

    for name, container in config_object.get("containers", {}).items():
        containers[name] = ContainerConfig(container.get("port"), container.get("type"), container.get("devices", []))
        if containers[name].port is None:
            raise RuntimeError("Container {} needs to have a port configured")

    registry = config_object.get("registry", None)
    if registry is None:
        raise RuntimeError("The Docker registry address has to be configured")

    return WorkerConfig(registry, containers)
