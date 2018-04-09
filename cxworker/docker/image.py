import logging

from cxworker.shepherd.config import RegistryConfig
from .utils import run_docker_command


class DockerImage:
    """Helper class for running and managing docker images."""

    def __init__(self, name: str, tag: str, registry: RegistryConfig):
        """
        Initialize new :py:class:`DockerImage`.

        :param name: image name, e.g.: ``cognexa/cxflow``
        :param tag: image tag, e.g.: ``latest`` or ``stable``
        :param registry: docker registry config
        """
        self._name: str = name
        self._tag: str = tag
        self._registry: RegistryConfig = registry

    @property
    def full_name(self) -> str:
        """Return docker image full name including registry url. E.g.: ``docker.cognexa.com/isletnet:latest``."""
        return '{registry}/{name}:{tag}'.format(registry=self._registry.schemeless_url, name=self._name, tag=self._tag)

    def pull(self) -> None:
        """Pull the underlying docker image."""
        self._login()
        logging.info('Pulling %s', self.full_name)
        run_docker_command(['pull', self.full_name])

    def _login(self) -> None:
        """If the registry configuration contains a username, log-in to the registry."""
        if self._registry.username is not None:
            logging.info('Logging to docker registry `%s` as `%s`', self._registry.url, self._registry.username)
            # the following command exposes docker registry username nad password!
            run_docker_command(['login', '-u', self._registry.username, '-p', self._registry.password,
                                self._registry.url])
