import logging
import re
import yaml
from typing import Optional, Dict, Any

from schematics import Model
from schematics.types import ModelType, DictType, StringType, URLType, BaseType


def strip_url_scheme(url):
    """
    >>> strip_url_scheme("https://google.com")
    'google.com'
    >>> strip_url_scheme("http://google.com")
    'google.com'
    >>> strip_url_scheme("google.com")
    'google.com'
    """

    match = re.match(r'[^:]+://(.*)', url)
    if match is None:
        return url
    return match.group(1)


class StorageConfig(Model):
    url: str = URLType(fqdn=False, required=True)
    access_key: str = StringType(required=True)
    secret_key: str = StringType(required=True)

    @property
    def schemeless_url(self):
        return strip_url_scheme(self.url)

    @property
    def secure(self) -> bool:
        return not self.url.startswith("http://")  # If no scheme is given


class RegistryConfig(Model):
    url: str = URLType(fqdn=False, required=True)
    username: Optional[str] = StringType(required=False)
    password: Optional[str] = StringType(required=False)

    @property
    def schemeless_url(self):
        return strip_url_scheme(self.url)


class LoggingConfig(Model):
    level: str = StringType(default="info")

    @property
    def log_level(self):
        return getattr(logging, self.level.upper())


class WorkerConfig(Model):
    data_root: str = StringType(required=True)
    storage: StorageConfig = ModelType(StorageConfig, required=True)
    logging: LoggingConfig = ModelType(LoggingConfig, required=False, default=LoggingConfig(dict(level='info')))
    sheep: Dict[str, Dict[str, Any]] = DictType(DictType(BaseType), required=True)
    registry: Optional[RegistryConfig] = ModelType(RegistryConfig, required=False)


def load_worker_config(config_stream) -> WorkerConfig:
    config_object = yaml.load(config_stream)
    config = WorkerConfig(config_object)
    config.validate()
    return config
