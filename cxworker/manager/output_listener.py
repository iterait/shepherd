from io import BytesIO

import logging
import requests
from minio import Minio

from .errors import ContainerError
from .registry import ContainerRegistry


class OutputListener:
    """
    Waits for output from containers and stores it
    """

    def __init__(self, registry: ContainerRegistry, minio: Minio):
        self.registry = registry
        self.minio = minio

    def listen(self):
        while True:
            container_ids = self.registry.wait_for_output()

            for id in container_ids:
                request = self.registry.get_current_request(id)

                if request is None:
                    continue  # TODO containers without a request should not send any output - investigate!

                try:
                    output = self.registry.read_output(id)
                    self.minio.put_object(request.id, request.result_url, BytesIO(output), len(output))

                    self._send_status(request, {
                        "success": True,
                        "status": "Done"
                    })
                except ContainerError as e:
                    logging.exception("Exception in container {}".format(id), e)

                    self._send_status(request, {
                        "success": False,
                        "status": str(e)
                    })

                self.registry.request_finished(id)

    @staticmethod
    def _send_status(request, data: dict):
        if request.status_url is not None:
            requests.post(request.status_url, json=data)
