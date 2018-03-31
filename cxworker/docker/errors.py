from ..sheep.errors import SheepError


class DockerError(SheepError):
    def __init__(self, msg, rc=None, output=None):
        if rc is not None and output is not None:
            super().__init__('{} (return code {}) with output:\n{}'.format(msg, rc, output))
        else:
            super().__init__(msg)
