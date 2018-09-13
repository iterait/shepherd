from typing import Optional

from ..sheep.errors import SheepError


class DockerError(SheepError):
    """Error in execution of a docker command."""

    def __init__(self, msg: str, rc: Optional[int]=None, output: Optional[str]=None):
        """
        Initialize new :py:class:`DockerError`.

        :param msg: error message
        :param rc: command return code
        :param output: command output
        """
        if rc is not None and output is not None:
            super().__init__('{} (return code {}) with output:\n{}'.format(msg, rc, output))
        else:
            super().__init__(msg)
