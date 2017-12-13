import yaml
from typing import Mapping, List


class ContainerConfig:
    def __init__(self, port: int, type: str, devices: List[str]):
        self.port = port
        self.type = type
        self.devices = devices


class StorageConfig:
    def __init__(self, url, access_key, secret_key, secure=True):
        self.url = url
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure


class RegistryConfig:
    def __init__(self, url, username=None, password=None):
        self.url = url
        self.username = username
        self.password = password


class WorkerConfig:
    def __init__(self, registry: RegistryConfig, storage: StorageConfig, containers: Mapping[str, ContainerConfig],
                 autoremove_containers: bool):
        self.registry = registry
        self.storage = storage
        self.containers = containers
        self.autoremove_containers = autoremove_containers


def load_config(config_stream) -> WorkerConfig:
    config_object = yaml.load(config_stream)
    containers = {}

    for name, container in config_object.get("containers", {}).items():
        containers[name] = ContainerConfig(container.get("port"), container.get("type"), container.get("devices", []))
        if containers[name].port is None:
            raise RuntimeError("Container {} needs to have a port configured")

    registry_config = config_object.get("registry", None)
    if registry_config is None:
        raise RuntimeError("The Docker registry address has to be configured")

    registry = RegistryConfig(**registry_config)

    storage_config = config_object.get("storage")
    if storage_config is None:
        raise RuntimeError("The storage must be configured")

    storage = StorageConfig(**storage_config)

    return WorkerConfig(registry, storage, containers, bool(config_object.get("autoremove_containers", True)))
