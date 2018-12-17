import re
from typing import Dict, Any, Optional, List

from schematics.types import BooleanType

from .base_sheep import BaseSheep
from ..docker import DockerContainer, DockerImage
from ..config import RegistryConfig
from ..errors.docker import DockerError
from ..errors.sheep import SheepConfigurationError


def extract_gpu_number(device_name: str) -> Optional[str]:
    """
    Extract GPU number from a Linux device name
    >>> extract_gpu_number("/dev/nvidia1")
    '1'
    >>> extract_gpu_number("/dev/sda2") is None
    True
    >>> extract_gpu_number("/dev/nvidiactl") is None
    True
    """
    match = re.match(r'/dev/nvidia([0-9]+)$', device_name)
    if match is not None:
        return match.group(1)
    return None


class DockerSheep(BaseSheep):
    """
    Sheep running its jobs in docker containers.
    To enable GPU computation, specify the gpu devices in the configuration and sheep will attempt to
    use ``nvidia docker 2``.
    """

    _CONTAINER_POINT = 9999
    """Container port to bind the socket to."""

    class Config(BaseSheep.Config):
        autoremove_containers: bool = BooleanType(default=False)

    def __init__(self, config: Dict[str, Any], registry_config: RegistryConfig,
                 command: Optional[List[str]]=None, **kwargs):
        """
        Create new :py:class:`DockerSheep`.

        :param config: docker sheep configuration
        :param registry_config: docker registry configuration
        :param command: optional docker container run command
        :param kwargs: :py:class:`BaseSheep`'s kwargs
        """
        super().__init__(**kwargs)
        self._config: self.Config = self.Config(config)
        self._registry_config = registry_config
        self._container: Optional[DockerContainer] = None
        self._image: Optional[DockerImage] = None
        self._command: Optional[List[str]] = command

    def _load_model(self, model_name: str, model_version: str) -> None:
        """
        Pull docker image of the given name and version from the previously configured docker registry.

        :param model_name: docker image name
        :param model_version: docker image version
        """
        super()._load_model(model_name, model_version)
        self._image = DockerImage(model_name, model_version, self._registry_config)
        try:
            self._image.pull()
        except DockerError as de:
            raise SheepConfigurationError('Specified model name `{}` (version `{}`) cannot be loaded.'
                                          .format(model_name, model_version)) from de

    def start(self, model_name: str, model_version: str) -> None:
        """
        Run a docker command starting the docker runner.

        :param model_name: docker image name
        :param model_version: docker image version
        """
        super().start(model_name, model_version)

        # prepare nvidia docker 2 env/runtime arguments (-e/--runtime)
        visible_gpu_numbers = list(filter(None, map(extract_gpu_number, self._config.devices)))
        env = {"NVIDIA_VISIBLE_DEVICES": ",".join(visible_gpu_numbers)}
        runtime = "nvidia" if visible_gpu_numbers else None

        # create and start :py:class:`DockerContainer`
        self._container = DockerContainer(self._image, self._config.autoremove_containers, env=env, runtime=runtime,
                                          bind_mounts={self.sheep_data_root: self.sheep_data_root},
                                          ports={self._config.port: self._CONTAINER_POINT}, command=self._command)
        try:
            self._container.start()
        except DockerError as de:
            self._container = None
            raise SheepConfigurationError('Specified model name `{}` (version `{}`) cannot be started.'
                                          .format(model_name, model_version)) from de

    def slaughter(self) -> None:
        """Kill the underlying docker container."""
        super().slaughter()
        if self._container is not None:
            self._container.kill()
            self._container = None

    @property
    def running(self) -> bool:
        """Check if the underlying docker container is running."""
        return self._container is not None and self._container.running
