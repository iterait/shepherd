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
            for container_id in self.registry.wait_for_output():
                request = self.registry.get_current_request(container_id)

                try:
                    output = self.registry.read_output(container_id)
                    self.minio.put_object(request.id, request.result_url, BytesIO(output), len(output))

                    self._send_status(request, {
                        "success": True,
                        "status": "Done"
                    })
                except ContainerError as e:
                    logging.exception("Exception in container {}, %s".format(str(container_id)), e)

                    self._send_status(request, {
                        "success": False,
                        "status": str(e)
                    })

                self.registry.request_finished(container_id)

    @staticmethod
    def _send_status(request, data: dict):
        if request.status_url is not None:
            try:
                requests.post(request.status_url, json=data)
            except ConnectionError:
                logging.error("Failed to report status for request `%s`", request.id)
        else:
            logging.warning("Cannot report status for task `%s` as status_url was not provided.", request.id)
