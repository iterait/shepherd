import json
from os import path
from time import sleep

from shepherd.runner import JSONRunner
from shepherd.constants import DEFAULT_PAYLOAD_FILE, DEFAULT_OUTPUT_FILE


class StressRunner(JSONRunner):
    """Stress test runner"""

    def _process_job(self, input_path: str, output_path: str) -> None:
        with open(path.join(input_path, DEFAULT_PAYLOAD_FILE), 'r') as file:
            payload = json.load(file)

        sleep(int(payload['sleep']))

        with open(path.join(output_path, DEFAULT_OUTPUT_FILE), 'w') as file:
            json.dump({'result': payload['input']}, file)
