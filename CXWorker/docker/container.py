import subprocess
import requests

from .errors import DockerError


class DockerContainer:
    def __init__(self, image_name: str, command: str = "docker"):
        """
        :param image_name: Name of the image from which the container will be created
        :param command: an alternate command used to manage containers (e.g. nvidia-docker)
        """

        self.image_name = image_name
        self.command = command
        self.ports = {}
        self.volumes = []
        self.devices = []
        self.container_id = None

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

        # Remove the container when it exits
        command.append("--rm")

        # Bind volumes
        for volume_spec in self.volumes:
            command.append("--volume {}".format(volume_spec))

        # Bind devices
        for device in self.devices:
            command.append("--device {}".format(device))

        # Positional args - the image of the container
        command.append(self.image_name)

        # Launch the container and wait until the "run" commands finishes
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        rc = process.wait()

        if rc != 0:
            raise DockerError('Running the container failed', rc, process.stderr.read())

        # Read the container ID from the standard output
        self.container_id = process.stdout.read().strip()

    def kill(self):
        """
        Kill the container
        """

        if self.container_id is None:
            raise DockerError('The container was not started yet')

        command = [self.command, 'kill', self.container_id]
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        rc = process.wait()

        if rc != 0:
            raise DockerError('Killing the container failed', rc, process.stderr.read())

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


class NvidiaDockerContainer(DockerContainer):
    def __init__(self, image_name: str):
        super().__init__(image_name, "nvidia-docker")

    def start(self):
        nvidia_metadata = requests.get('http://localhost:3476/docker/cli/json').json()
        volumes = nvidia_metadata.get("Volumes", [])

        for volume_spec in volumes:
            self.add_volume(volume_spec)

        super().start()
