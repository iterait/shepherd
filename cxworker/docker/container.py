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
        self.devices = []
        self.container_id = None
        self.runtime = runtime
        self.env = env or {}
        self.mounts = {}

    def add_port_mapping(self, host_port, container_port):
        """
        Map a port on the host machine to given port on the container

        :param host_port:
        :param container_port:
        """
        self.ports[host_port] = container_port

    def add_bind_mount(self, host_path, container_path):
        self.mounts[host_path] = container_path

    def add_device(self, name):
        self.devices.append(name)

    def start(self):
        """
        Run the container
        """

        # Run given image in detached mode
        command = ['docker', 'run', '-d']

        # Add configured port mappings
        for host_port, container_port in self.ports.items():
            command += ['-p', '0.0.0.0:{host}:{container}'.format(host=host_port, container=container_port)]
            DockerContainer.kill_blocking_container(host_port)

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

        # Bind mount
        for host_path, container_path in self.mounts.items():
            command.append("--mount")
            command.append(','.join(['='.join([key, value])
                                     for key, value in (('type', 'bind'),
                                                        ('source', host_path),
                                                        ('target', container_path))]))

        # Bind devices
        for device in self.devices:
            command.append("--device")
            command.append(device)

        # Positional args - the image of the container
        command.append(self.image.full_name)

        # Launch the container and wait until the "run" commands finishes
        logging.debug("Running command %s", str(command))
        print(' '.join(command))
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
