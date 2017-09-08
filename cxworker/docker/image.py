import subprocess

from .errors import DockerError


class DockerImage:
    def __init__(self, name, tag, registry):
        self.name = name
        self.tag = tag
        self.registry = registry

    def pull(self):
        process = subprocess.Popen([
            'docker',
            'pull',
            '{registry}/{name}:{tag}'.format(registry=self.registry, tag=self.tag, name=self.name)
        ], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

        rc = process.wait()

        if rc != 0:
            raise DockerError('Pulling the image failed', rc, process.stderr.read())
