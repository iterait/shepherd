import subprocess
import requests
import logging

from .errors import DockerError


class DockerContainer:
    def __init__(self, repository_name: str, image_name: str, autoremove: bool, command: str = "docker", runtime: str = None):
        """
        :param repository_name: Name of the repository where the image is contained
        :param image_name: Name of the image from which the container will be created
        :param command: an alternate command used to manage containers (e.g. nvidia-docker)
        """

        self.repository_name = repository_name
        self.image_name = image_name
        self.autoremove = autoremove
        self.command = command
        self.ports = {}
        self.volumes = []
        self.devices = []
        self.container_id = None
        self.runtime = runtime

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
        command = [self.command, 'run', '-d']

        # Add configured port mappings
        for host_port, container_port in self.ports.items():
            command += ['-p', '127.0.0.1:{host}:{container}'.format(host=host_port, container=container_port)]

        # If desired, remove the container when it exits
        if self.autoremove:
            command.append("--rm")

        # Bind volumes
        for volume_spec in self.volumes:
            command.append("--volume")
            command.append(volume_spec)

        # Bind devices
        for device in self.devices:
            command.append("--device")
            command.append(device)

        # Positional args - the image of the container
        command.append("{}/{}".format(self.repository_name, self.image_name))

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

        command = [self.command, 'kill', self.container_id]
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

        command = [self.command, 'ps', '--filter', 'id={}'.format(self.container_id.decode())]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        rc = process.wait()

        if rc != 0:
            raise DockerError('Checking the status of the container failed', rc, process.stderr.read())

        # If the command output contains more than one line, the container was found (the first line is a header)
        return len(process.stdout.readlines()) > 1


class LegacyNvidiaDockerContainer(DockerContainer):
    def __init__(self, repository: str, image_name: str, autoremove: bool):
        super().__init__(repository, image_name, autoremove, "nvidia-docker")

    def start(self):
        nvidia_metadata = requests.get('http://localhost:3476/docker/cli/json').json()
        volumes = nvidia_metadata.get("Volumes", [])

        for volume_spec in volumes:
            self.add_volume(volume_spec)

        self.add_device("/dev/nvidia-uvm")
        self.add_device("/dev/nvidiactl")

        super().start()


class NvidiaDockerContainer(DockerContainer):
    def __init__(self, repository: str, image_name: str, autoremove: bool):
        super().__init__(repository, image_name, autoremove, runtime="nvidia")
