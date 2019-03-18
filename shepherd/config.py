import logging
import re
import os
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
    url: str = StringType(required=True)
    access_key: str = StringType(required=True)
    secret_key: str = StringType(required=True)

    @property
    def schemeless_url(self):
        return strip_url_scheme(self.url)

    @property
    def secure(self) -> bool:
        return self.url.startswith("https://")  # If no scheme is given


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


class ShepherdConfig(Model):
    data_root: str = StringType(required=True)
    storage: StorageConfig = ModelType(StorageConfig, required=True)
    logging: LoggingConfig = ModelType(LoggingConfig, required=False, default=LoggingConfig(dict(level='info')))
    sheep: Dict[str, Dict[str, Any]] = DictType(DictType(BaseType), required=True)
    registry: Optional[RegistryConfig] = ModelType(RegistryConfig, required=False)


def load_shepherd_config(config_stream) -> ShepherdConfig:
    # regex pattern for compiling ENV variables
    regex_no_brackets = re.compile(r'([^$]*)\$([A-Z_][A-Z_0-9]*)')
    regex_brackets = re.compile(r'([^$]*)\${([A-Z_][A-Z_0-9]*)}')
    yaml.add_implicit_resolver('!env', regex_no_brackets)
    yaml.add_implicit_resolver('!env', regex_brackets)

    # define constructor for recognizing environment variables
    def env_constructor(loader, node):
        value = loader.construct_scalar(node)

        def replace_env_vars(matchobj):
            env_name = matchobj.group(2)
            if env_name not in os.environ:
                raise ValueError(f'Environment variable `{env_name}` not set')
            return matchobj.group(1) + os.environ[env_name]

        value = regex_no_brackets.sub(replace_env_vars, value)
        value = regex_brackets.sub(replace_env_vars, value)
        return value

    # add constructor to yaml loader
    yaml.add_constructor('!env', env_constructor)

    # construct config object
    config_object = yaml.load(config_stream)

    config = ShepherdConfig(config_object)
    config.validate()
    return config
