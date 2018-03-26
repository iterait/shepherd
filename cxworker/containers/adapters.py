import subprocess
import os
import abc
import re
import shlex
from typing import Dict, Any, NamedTuple, List, Optional, Sequence

from cxworker.docker import DockerContainer, DockerImage
from cxworker.errors import ContainerConfigurationError
from cxworker.manager.config import RegistryConfig


class ContainerAdapter(metaclass=abc.ABCMeta):
    """
    A base class for container adapters - classes that allow launching different kinds of containers.
    """

    class Config(NamedTuple):
        type: str
        port: int
        devices: List[str] = []

    config: Config

    def __init__(self, config: Dict[str, Any]):
        pass

    @abc.abstractmethod
    def load_model(self, model_name: str, model_version: str):
        """
        Load a model
        """

    @abc.abstractmethod
    def update_model(self) -> bool:
        """
        Update the model loaded of the underlying container. The container should not be restarted.
        :return: True if there was an update, False otherwise
        """

    @abc.abstractmethod
    def start(self, slaves: Sequence['ContainerAdapter']):
        """
        Start the underlying container and make it listen on the configured port.
        The load_model method must always be called before this.
        """

    @abc.abstractmethod
    def kill(self):
        """
        Kill the underlying container.
        """

    @property
    @abc.abstractmethod
    def running(self) -> bool:
        """
        Is the container running?
        """


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


class DockerAdapter(ContainerAdapter):
    """
    A container adapter with a Docker backend. If the user links nvidia devices into it, the nvidia runtime
    (a.k.a. nvidia-docker2) is used.
    """

    WORKER_PROCESS_PORT = 9999

    class Config(ContainerAdapter.Config):
        autoremove_containers: bool = True

    def __init__(self, config: Dict[str, Any], registry_config: RegistryConfig):
        super().__init__(config)
        self.config: self.Config = self.Config(**config)
        self.registry_config = registry_config
        self.container: Optional[DockerContainer] = None
        self.image: Optional[DockerImage] = None

    def load_model(self, model_name: str, model_version: str):
        self.image = DockerImage(model_name, model_version, self.registry_config)
        self.image.pull()

    def update_model(self) -> bool:
        return self.image.pull()

    def start(self, slaves: Sequence[ContainerAdapter]):
        devices = self.config.devices
        for slave in slaves:
            devices.extend(slave.config.devices)

        visible_gpu_numbers = list(filter(None, map(extract_gpu_number, devices)))
        env = {"NVIDIA_VISIBLE_DEVICES": ",".join(visible_gpu_numbers)}

        self.container = DockerContainer(self.image, self.config.autoremove_containers, env=env,
                                         runtime="nvidia" if visible_gpu_numbers else None)
        self.container.add_port_mapping(self.config.port, self.WORKER_PROCESS_PORT)
        self.container.start()

    def kill(self):
        if self.container is not None:
            self.container.kill()

    @property
    def running(self) -> bool:
        return self.container is not None and self.container.running


class BareAdapter(ContainerAdapter):
    """
    An adapter that can only run one type of the model on bare metal.
    This might be useful when Docker isolation is impossible or not necessary, for example in deployments with just a
    few models.
    """

    class Config(NamedTuple):
        type: str
        port: int
        model_name: str
        model_version: str
        config_path: str
        working_directory: str
        stdout_file: Optional[str] = None
        stderr_file: Optional[str] = None
        devices: List[str] = []

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.config: self.Config = self.Config(**config)
        self.process = None

    def load_model(self, model_name: str, model_version: str):
        if model_name != self.config.model_name:
            raise ContainerConfigurationError("This container can only load model '{}'".format(model_name))

        if model_version != self.config.model_version:
            raise ContainerConfigurationError("This container can only load version '{}' of model '{}'"
                                              .format(model_name, model_version))

    def update_model(self):
        pass

    def start(self, slaves: Sequence[ContainerAdapter]):
        stdout = open(self.config.stdout_file, 'a') if self.config.stdout_file is not None else subprocess.DEVNULL
        stderr = open(self.config.stderr_file, 'a') if self.config.stderr_file is not None else subprocess.DEVNULL

        devices = self.config.devices
        for slave in slaves:
            devices.extend(slave.config.devices)

        env = os.environ.copy()
        env["CUDA_VISIBLE_DEVICES"] = ",".join(filter(None, map(extract_gpu_number, devices)))

        self.process = subprocess.Popen(
            shlex.split("cxworker-runner -p {} {}".format(self.config.port, self.config.config_path)), env=env,
            cwd=self.config.working_directory, stdout=stdout, stderr=stderr)

    def kill(self):
        if self.process is not None:
            self.process.kill()

    @property
    def running(self) -> bool:
        return self.process is not None and self.process.poll() is None
