import logging
import subprocess
from typing import Dict, Optional, List

from cxworker.docker import DockerImage
from .errors import DockerError


class DockerContainer:
    """
    Helper class for running and managing docker containers.
    """

    def __init__(self,
                 image: DockerImage,
                 autoremove: bool=True,
                 runtime: Optional[str]=None,
                 env: Optional[Dict[str, str]]=None,
                 bind_mounts: Optional[Dict[str, str]]=None,
                 ports: Optional[Dict[str, str]]=None):
        """
        Initialize :py:class`DockerContainer`.

        :param image: container :py:class:`DockerImage`
        :param autoremove: remove the container after it is stopped
        :param runtime: docker runtime flag (e.g. ``nvidia``)
        :param env: additional environment variables
        :param bind_mounts: optional host->container bind mounts mapping
        """
        self._image = image
        self._autoremove = autoremove
        self._container_id: Optional[str] = None
        self._runtime: Optional[str] = runtime
        self._env: Dict = env or {}
        self._mounts: Dict = bind_mounts or {}
        self._ports: Dict = ports or {}

    def add_port_mapping(self, host_port, container_port):
        self._ports[host_port] = container_port

    def start(self):
        """
        Run the container
        """

        # Run given image in detached mode
        command = ['run', '-d']

        # Add configured port mappings
        for host_port, container_port in self._ports.items():
            command += ['-p', '0.0.0.0:{host}:{container}'.format(host=host_port, container=container_port)]
            DockerContainer.kill_blocking_container(host_port)

        # Set environment variables
        if self._env:
            command.append("-e")

            for key, value in self._env.items():
                command.append("{}={}".format(key, value))

        # If desired, remove the container when it exits
        if self._autoremove:
            command.append("--rm")

        # Set runtime
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

        self._container_id = DockerContainer.run_docker_command(command)

    def kill(self):
        """
        Kill the container.
        """
        if self._container_id is None:
            raise DockerError('The container was not started yet')
        DockerContainer.run_docker_command(['kill', self._container_id])
        self._container_id = None

    @property
    def running(self):
        """
        :return: True when the container is running, False otherwise
        """
        if self._container_id is None:
            raise DockerError('The container was not started yet')
        output = DockerContainer.run_docker_command(['ps', '--filter', 'id={}'.format(self._container_id)])
        # If the command output contains more than one line, the container was found (the first line is a header)
        return len(output.split('\n')) > 1

    @staticmethod
    def kill_blocking_container(host_port: int) -> None:
        """
        List all the running docker container mapping and attempt kill any container holding the given port.

        :param host_port: host port to be freed
        """
        host_port = str(host_port)
        process = subprocess.Popen(["docker", "ps", "--format", "{{.Ports}}\t{{.Names}}"],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()
        ps_info = process.stdout.read().decode()
        for ps_line in ps_info.split('\n'):
            if len(ps_line.strip()) == 0:
                continue
            port_mappings, name = ps_line.split('\t')
            for port_mapping in port_mappings.split(','):
                host_port_held = port_mapping.split(':')[1].split('->')[0]
                if host_port_held == host_port:
                    logging.info('Killing docker container `%s` as it holds port %s', name, host_port)
                    killing_process = subprocess.Popen(['docker', 'kill', name])
                    killing_process.wait()
                    return

    @staticmethod
    def run_docker_command(command: List[str]) -> str:
        """
        Run and wait the given docker command. Return its stdout.

        :param command: docker command to be run as a lex list
        :raise DockerError: on failure
        :return: command stdout
        """
        command = ['docker'] + command
        plain_command = ' '.join(command)
        logging.debug('Running command `%s`', plain_command)
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return_code = process.wait()
        stderr = process.stderr.read().decode()

        if len(stderr):
            logging.warning("Non-empty stderr when running command `%s`: %s", plain_command, stderr)
        if return_code != 0:
            raise DockerError('Running command `{}` failed.'.format(plain_command), return_code, stderr)

        stdout = process.stdout.read().decode().strip()
        logging.debug("Running command `%s` yielded output: %s", plain_command, stdout)
        return stdout
