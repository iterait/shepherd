import os
import shlex
import subprocess
import os.path as path
from typing import Dict, Any, Optional

import cxflow as cx
from schematics.types import StringType

from .base_sheep import BaseSheep
from .docker_sheep import extract_gpu_number
from cxworker.sheep.errors import SheepConfigurationError


class BareSheep(BaseSheep):
    """
    An adapter that running models on bare metal with ``cxworker-runner``.
    This might be useful when Docker isolation is impossible or not necessary, for example in deployments with just a
    few models.
    """

    class Config(BaseSheep.Config):
        working_directory: str = StringType(required=True)  # working directory of the cxworker-runner
        stdout_file: Optional[str] = StringType(required=False)  # if specified, capture runner's stdout to this file
        stderr_file: Optional[str] = StringType(required=False)  # if specified, capture runner's stderr to this file

    def __init__(self, config: Dict[str, Any], **kwargs):
        """
        Create new :py:class:`BareSheep`.

        :param config: bare sheep configuration (:py:class:`BareSheep.Config`)
        :param kwargs: parent's kwargs
        """
        super().__init__(**kwargs)
        self._config: self.Config = self.Config(config)
        self._runner: Optional[subprocess.Popen] = None
        self._runner_config_path: Optional[str] = None

    def _load_model(self, model_name: str, model_version: str) -> None:
        """
        Set up runner config path to ``working_directory`` / ``model_name`` / ``model_version`` / ``config.yaml``.

        :param model_name: model name
        :param model_version: model version
        :raise SheepConfigurationError: if the runner config path does not exist
        """
        cxflow_config_path = path.join(self._config.working_directory, model_name, model_version,
                                       cx.constants.CXF_CONFIG_FILE)
        if not path.exists(cxflow_config_path):
            raise SheepConfigurationError("Cannot load model `{}:{}`, file '{}' does not exist."
                                          .format(model_name, model_version, cxflow_config_path))
        super()._load_model(model_name, model_version)
        self._runner_config_path = path.relpath(cxflow_config_path, self._config.working_directory)

    def start(self, model_name: str, model_version: str) -> None:
        """
        Start a subprocess with the sheep runner.

        :param model_name: model name
        :param model_version: model version
        """
        super().start(model_name, model_version)

        # prepare env. variables for GPU computation and stdout/stderr files
        env = os.environ.copy()
        env["CUDA_VISIBLE_DEVICES"] = ",".join(filter(None, map(extract_gpu_number, self._config.devices)))
        stdout = subprocess.DEVNULL
        if self._config.stdout_file is not None:
            os.makedirs(os.path.dirname(self._config.stdout_file), exist_ok=True)
            stdout = open(self._config.stdout_file, 'a')
        stderr = subprocess.DEVNULL
        if self._config.stderr_file is not None:
            os.makedirs(os.path.dirname(self._config.stderr_file), exist_ok=True)
            stderr = open(self._config.stderr_file, 'a')

        # start the runner in a new sub-process
        self._runner = subprocess.Popen(
            shlex.split("cxworker-runner -p {} {}".format(self._config.port, self._runner_config_path)), env=env,
            cwd=self._config.working_directory, stdout=stdout, stderr=stderr)

    def slaughter(self) -> None:
        """Kill the underlying runner (subprocess)."""
        super().slaughter()
        if self._runner is not None:
            self._runner.kill()
            self._runner = None

    @property
    def running(self) -> bool:
        """Check if the underlying runner (subprocess) is running."""
        return self._runner is not None and self._runner.poll() is None
