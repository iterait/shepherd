from minio import Minio
from io import BytesIO
import requests
import time
import json

time.sleep(2)

request_id = 'docker-test-request'
source_url = 'input.json'
output_url = 'output.json'
container_id = "container_a"

input_data = b'{"key":[42]}'

minio = Minio('0.0.0.0:7000', 'AKIAIOSFODNN7EXAMPLE', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY', False)
if not minio.bucket_exists(request_id):
    minio.make_bucket(request_id)
minio.put_object(request_id, source_url, BytesIO(input_data), len(input_data))

configuration = {"container_id": container_id, "model": {"name": "cxflow-test", "version": "latest" } }
resp = requests.post('http://0.0.0.0:5000/reconfigure', json=configuration)
print(resp.status_code, resp.content)
assert resp.status_code == 200

task = {"id": request_id, "result_url": output_url, "source_url": source_url, "container_id": container_id}
resp = requests.post('http://0.0.0.0:5000/start-job', json=task)
print(resp.status_code, resp.content)
assert resp.status_code == 200

time.sleep(2)
output = json.loads(minio.get_object(request_id, output_url).read().decode())
assert output['key'] == [42]
assert output['output'] == [999]
print('Docker test OK.')
