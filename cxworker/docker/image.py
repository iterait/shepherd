import subprocess

from cxworker.manager.config import RegistryConfig
from .errors import DockerError


class DockerImage:
    def __init__(self, name, tag, registry: RegistryConfig):
        self.name = name
        self.tag = tag
        self.registry = registry

    def pull(self):
        if self.registry.username is not None:
            process = subprocess.Popen([
                'docker',
                'login',
                '-u',
                self.registry.username,
                '--password-stdin',
                self.registry.url
            ], stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL)

            process.stdin.write(self.registry.password.encode())
            process.stdin.close()

            rc = process.wait()

            if rc != 0:
                raise DockerError('Logging in to the registry failed', rc, process.stderr.read())

        process = subprocess.Popen([
            'docker',
            'pull',
            '{registry}/{name}:{tag}'.format(registry=self.registry.url, tag=self.tag, name=self.name)
        ], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

        rc = process.wait()

        if rc != 0:
            raise DockerError('Pulling the image failed', rc, process.stderr.read())
