import logging
import subprocess
from typing import Dict

from cxworker.docker import DockerImage
from .errors import DockerError


class DockerContainer:
    def __init__(self, image: DockerImage, autoremove: bool, runtime: str = None, env: Dict[str, str] = None):
        self.image = image
        self.autoremove = autoremove
        self.ports = {}
        self.volumes = []
        self.devices = []
        self.container_id = None
        self.runtime = runtime
        self.env = env or {}

    def add_port_mapping(self, host_port, container_port):
        """
        Map a port on the host machine to given port on the container
        :param host_port:
        :param container_port:
        """

        self.ports[host_port] = container_port

    def add_device(self, name):
        self.devices.append(name)

    def add_volume(self, volume_spec):
        self.volumes.append(volume_spec)

    def start(self):
        """
        Run the container
        """

        # Run given image in detached mode
        command = ['docker', 'run', '-d']

        # Add configured port mappings
        for host_port, container_port in self.ports.items():
            command += ['-p', '127.0.0.1:{host}:{container}'.format(host=host_port, container=container_port)]

        # Set environment variables
        if self.env:
            command.append("-e")

            for key, value in self.env.items():
                command.append("{}={}".format(key, value))

        # If desired, remove the container when it exits
        if self.autoremove:
            command.append("--rm")

        # Set runtime
        if self.runtime:
            command.append("--runtime={}".format(self.runtime))

        # Bind volumes
        for volume_spec in self.volumes:
            command.append("--volume")
            command.append(volume_spec)

        # Bind devices
        for device in self.devices:
            command.append("--device")
            command.append(device)

        # Positional args - the image of the container
        command.append(self.image.full_name)

        # Launch the container and wait until the "run" commands finishes
        logging.debug("Running command %s", str(command))
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        rc = process.wait()
        stderr = process.stderr.read()

        if len(stderr):
            logging.warning("Non-empty stderr when starting container: %s", stderr)
        if rc != 0:
            raise DockerError('Running the container failed', rc, stderr)

        # Read the container ID from the standard output
        stdout = process.stdout.read().strip()
        logging.debug("Received '%s' from docker", stdout)

        self.container_id = stdout

    def kill(self):
        """
        Kill the container
        """

        if self.container_id is None:
            raise DockerError('The container was not started yet')

        command = ['docker', 'kill', self.container_id]
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        rc = process.wait()
        stderr = process.stderr.read()

        if len(stderr):
            logging.warning("Non-empty stderr when killing container: %s", stderr)
        if rc != 0:
            raise DockerError('Killing the container failed', rc, stderr)

        self.container_id = None

    @property
    def running(self):
        """
        :return: True when the container is running, False otherwise
        """

        if self.container_id is None:
            raise DockerError('The container was not started yet')

        command = ['docker', 'ps', '--filter', 'id={}'.format(self.container_id.decode())]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        rc = process.wait()

        if rc != 0:
            raise DockerError('Checking the status of the container failed', rc, process.stderr.read())

        # If the command output contains more than one line, the container was found (the first line is a header)
        return len(process.stdout.readlines()) > 1
