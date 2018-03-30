import re
from typing import Dict, Any, Optional

from schematics.types import BooleanType

from .base_sheep import BaseSheep
from ..docker import DockerContainer, DockerImage
from ..manager.config import RegistryConfig

__all__ = ['DockerSheep', 'extract_gpu_number']


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
    A container adapter with a Docker backend. If the user links nvidia devices into it, the nvidia runtime
    (a.k.a. nvidia-docker2) is used.
    """

    CONTAINER_PORT = 9999
    """Container port to bind the socket to."""

    class Config(BaseSheep.Config):
        autoremove_containers: bool = BooleanType(default=False)

    def __init__(self, config: Dict[str, Any], registry_config: RegistryConfig, **kwargs):
        super().__init__(**kwargs)
        self.config: self.Config = self.Config(config)
        self.registry_config = registry_config
        self.container: Optional[DockerContainer] = None
        self.image: Optional[DockerImage] = None

    def load_model(self, model_name: str, model_version: str):
        super().load_model(model_name, model_version)
        self.image = DockerImage(model_name, model_version, self.registry_config)
        self.image.pull()

    def update_model(self) -> bool:
        return self.image.pull()

    def start(self, model_name: str, model_version: str):
        super().start(model_name, model_version)
        devices = self.config.devices

        visible_gpu_numbers = list(filter(None, map(extract_gpu_number, devices)))
        env = {"NVIDIA_VISIBLE_DEVICES": ",".join(visible_gpu_numbers)}

        self.container = DockerContainer(self.image, self.config.autoremove_containers, env=env,
                                         runtime="nvidia" if visible_gpu_numbers else None)
        self.container.add_port_mapping(self.config.port, self.CONTAINER_PORT)
        self.container.add_bind_mount(self.sheep_data_root, self.sheep_data_root)
        self.container.start()

    def slaughter(self):
        super().slaughter()
        if self.container is not None:
            self.container.kill()

    @property
    def running(self) -> bool:
        return self.container is not None and self.container.running
