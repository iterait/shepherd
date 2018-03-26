import logging
import yaml
from typing import NamedTuple, Optional, Dict, Any


class StorageConfig(NamedTuple):
    url: str
    access_key: str
    secret_key: str
    secure: bool = True


class RegistryConfig(NamedTuple):
    url: str
    username: Optional[str] = None
    password: Optional[str] = None


class LoggingConfig(NamedTuple):
    level: str = "info"

    @property
    def log_level(self):
        return getattr(logging, self.level.upper())


class WorkerConfig(NamedTuple):
    registry: RegistryConfig
    storage: StorageConfig
    logging: LoggingConfig
    containers: Dict[str, Dict[str, Any]]


def load_config(config_stream) -> WorkerConfig:
    config_object = yaml.load(config_stream)
    containers = {}

    for name, container in config_object.get("containers", {}).items():
        containers[name] = container

    registry_config = config_object.get("registry", {})
    registry = RegistryConfig(**registry_config)

    storage_config = config_object.get("storage", {})
    storage = StorageConfig(**storage_config)

    logging_config = config_object.get("logging", {})
    logging = LoggingConfig(**logging_config)

    return WorkerConfig(registry, storage, logging, containers)
