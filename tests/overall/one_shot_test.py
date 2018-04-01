from minio import Minio
from io import BytesIO
import logging
import requests
import time
import json
import multiprocessing
logging.basicConfig(level=logging.DEBUG)
pool = multiprocessing.Pool(4)

time.sleep(1)

source_url = 'input.json'
output_url = 'output.json'
container_id = "container_a"

input_data = b'{"key":[42]}'

minio = Minio('0.0.0.0:7000', 'AKIAIOSFODNN7EXAMPLE', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY', False)

configuration = {"sheep_id": container_id, "model": {"name": "cxflow-test", "version": "latest"}}
resp = requests.post('http://0.0.0.0:5000/reconfigure', json=configuration)
assert resp.status_code == 200

NUM = 10

for i in range(NUM):
    request_id = 'test-request'+str(i)
    if minio.bucket_exists(request_id):
        for obj in minio.list_objects_v2(request_id, recursive=True):
            minio.remove_object(request_id, obj.object_name)
        minio.remove_bucket(request_id)
    minio.make_bucket(request_id)
    data = json.dumps({'key': [i]}).encode()
    minio.put_object(request_id, source_url, BytesIO(data), len(data))

    task = {"job_id": request_id, "sheep_id": container_id}
    logging.info('Calling start-job end-point for %s', request_id)
    resp = requests.post('http://0.0.0.0:5000/start-job', json=task)
    assert resp.status_code == 200

time.sleep(3)

for i in range(NUM):
    request_id = 'test-request'+str(i)
    logging.info('Checking results of %s', request_id)
    output = json.loads(minio.get_object(request_id, output_url).read().decode())
    assert output['key'] == [i]
    assert output['output'] == [i*2]
