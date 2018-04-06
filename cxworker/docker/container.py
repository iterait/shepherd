import logging
from typing import Dict, Optional, List

from .image import DockerImage
from .errors import DockerError
from .utils import run_docker_command, kill_blocking_container


class DockerContainer:
    """Helper class for running and managing docker containers."""

    def __init__(self,
                 image: DockerImage,
                 autoremove: bool=True,
                 runtime: Optional[str]=None,
                 env: Optional[Dict[str, str]]=None,
                 bind_mounts: Optional[Dict[str, str]]=None,
                 ports: Optional[Dict[int, int]]=None,
                 command: Optional[List[str]]=None):
        """
        Initialize :py:class`DockerContainer`.

        :param image: container :py:class:`DockerImage`
        :param autoremove: remove the container after it is stopped
        :param runtime: docker runtime flag (e.g. ``nvidia``)
        :param env: additional environment variables
        :param bind_mounts: optional host->container bind mounts mapping
        :param ports: optional host->container port mapping
        :param command: optional docker container run command
        """
        self._image = image
        self._autoremove = autoremove
        self._container_id: Optional[str] = None
        self._runtime: Optional[str] = runtime
        self._env: Dict = env or {}
        self._mounts: Dict = bind_mounts or {}
        self._ports: Dict = ports or {}
        self._command: Optional[List[str]] = command

    def _build_run_command(self) -> List[str]:
        """
        Build docker container run command.

        :return: built command
        """
        # Run given image in detached mode
        command = ['run', '-d']

        # Add configured port mappings
        for host_port, container_port in self._ports.items():
            command += ['-p', '0.0.0.0:{host}:{container}'.format(host=host_port, container=container_port)]
            kill_blocking_container(host_port)

        # Set environment variables
        if self._env:
            command.append("-e")

            for key, value in self._env.items():
                command.append("{}={}".format(key, value))

        # If specified, remove the container when it exits
        if self._autoremove:
            command.append("--rm")

        # If specified, set the runtime (e.g. `nvidia`)
        if self._runtime:
            command.append("--runtime={}".format(self._runtime))

        # Bind mount
        for host_path, container_path in self._mounts.items():
            command.append("--mount")
            command.append(','.join(['='.join([key, value])
                                     for key, value in (('type', 'bind'),
                                                        ('source', host_path),
                                                        ('target', container_path))]))

        # Positional args - the image of the container
        command.append(self._image.full_name)

        # If specified, append the run command
        if self._command is not None:
            command += self._command

        return command

    def start(self) -> None:
        """Run the container."""
        command = self._build_run_command()
        logging.info('Starting docker container with `%s`', ' '.join(command))
        self._container_id = run_docker_command(command).strip()
        logging.info('Started docker container `%s`', self._container_id)

    def kill(self) -> None:
        """
        Kill the underlying docker container.

        :raise DockerError: if the container was not started yet (i.e., its ``container_id`` is not known)
        """
        if self._container_id is None:
            raise DockerError('The container was not started yet')
        logging.info('Killing container `%s`', self._container_id)
        run_docker_command(['kill', self._container_id])
        self._container_id = None

    @property
    def running(self) -> bool:
        """
        Check if the underlying docker container is still up and running.
        Returns ``False`` if the container was not even started.

        :return: docker container running flag
        """
        if self._container_id is None:
            return False
        output = run_docker_command(['ps', '--filter', 'id={}'.format(self._container_id)])
        # If the command output contains more than one line, the container was found (the first line is a header)
        return len(output.split('\n')) > 1
