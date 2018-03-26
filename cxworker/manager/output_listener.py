from io import BytesIO

import logging
import requests
from minio import Minio

from cxworker.manager.shepherd import Shepherd
from .errors import SheepError


class OutputListener:
    """
    Waits for output from sheep and stores it
    """

    def __init__(self, shepherd: Shepherd, minio: Minio):
        self.shepherd = shepherd
        self.minio = minio

    def listen(self):
        while True:
            for sheep_id in self.shepherd.wait_for_output():
                request = self.shepherd.get_current_request(sheep_id)

                try:
                    output = self.shepherd.read_output(sheep_id)
                    self.minio.put_object(request.id, request.result_url, BytesIO(output), len(output))

                    self._send_status(request, {
                        "success": True,
                        "status": "Done"
                    })
                except SheepError as e:
                    logging.exception("Exception in sheep {}, %s".format(str(sheep_id)), e)

                    self._send_status(request, {
                        "success": False,
                        "status": str(e)
                    })

                self.shepherd.request_finished(sheep_id)

    @staticmethod
    def _send_status(request, data: dict):
        if request.status_url is not None:
            try:
                requests.post(request.status_url, json=data)
            except ConnectionError:
                logging.error("Failed to report status for request `%s`", request.id)
        else:
            logging.warning("Cannot report status for task `%s` as status_url was not provided.", request.id)
