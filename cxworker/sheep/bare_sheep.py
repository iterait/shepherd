import os
import shlex
import subprocess
from typing import Dict, Any, Optional

from schematics.types import StringType

from .base_sheep import BaseSheep
from .docker_sheep import extract_gpu_number
from ..errors import SheepConfigurationError


__all__ = ['BareSheep']


class BareSheep(BaseSheep):
    """
    An adapter that can only run one type of the model on bare metal.
    This might be useful when Docker isolation is impossible or not necessary, for example in deployments with just a
    few models.
    """

    class Config(BaseSheep.Config):
        model_name: str = StringType(required=True)
        model_version: str = StringType(required=True)
        config_path: str = StringType(required=True)
        working_directory: str = StringType(required=True)
        stdout_file: Optional[str] = StringType(required=False)
        stderr_file: Optional[str] = StringType(required=False)

    def __init__(self, config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config: self.Config = self.Config(config)
        self.process = None

    def load_model(self, model_name: str, model_version: str):
        if model_name != self.config.model_name:
            raise SheepConfigurationError("This sheep can only load model '{}'".format(model_name))

        if model_version != self.config.model_version:
            raise SheepConfigurationError("This sheep can only load version '{}' of model '{}'"
                                          .format(model_name, model_version))
        super().load_model(model_name, model_version)

    def update_model(self):
        pass

    def start(self, model_name: str, model_version: str):
        super().start(model_name, model_version)
        stdout = open(self.config.stdout_file, 'a') if self.config.stdout_file is not None else subprocess.DEVNULL
        stderr = open(self.config.stderr_file, 'a') if self.config.stderr_file is not None else subprocess.DEVNULL

        devices = self.config.devices

        env = os.environ.copy()
        env["CUDA_VISIBLE_DEVICES"] = ",".join(filter(None, map(extract_gpu_number, devices)))

        self.process = subprocess.Popen(
            shlex.split("cxworker-runner -p {} {}".format(self.config.port, self.config.config_path)), env=env,
            cwd=self.config.working_directory, stdout=stdout, stderr=stderr)

    def slaughter(self):
        super().slaughter()
        if self.process is not None:
            self.process.kill()

    @property
    def running(self) -> bool:
        return self.process is not None and self.process.poll() is None

