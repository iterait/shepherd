from minio import Minio
from minio.error import MinioError
from io import BytesIO
import logging
import requests
import time
import json
import sys
import multiprocessing
logging.basicConfig(level=logging.DEBUG)
pool = multiprocessing.Pool(4)

time.sleep(1)

source_url = 'inputs/input.json'
output_url = 'outputs/output.json'
container_id = "container_a"

input_data = b'{"key":[42]}'

minio = Minio('0.0.0.0:7000', 'AKIAIOSFODNN7EXAMPLE', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY', False)

offset = 0
if len(sys.argv) > 1:
    offset = int(sys.argv[1])
NUM = 5

for i in range(NUM):
    request_id = 'test-request'+str(i+offset)
    if minio.bucket_exists(request_id):
        for obj in minio.list_objects_v2(request_id, recursive=True):
            minio.remove_object(request_id, obj.object_name)
        minio.remove_bucket(request_id)
    minio.make_bucket(request_id)
    data = json.dumps({'key': [i]}).encode()
    minio.put_object(request_id, source_url, BytesIO(data), len(data))

    task = {"job_id": request_id, "model": {"name": "cxflow-test", "version": "latest"+str(i)}}
    logging.info('Calling start-job end-point for %s', request_id)
    resp = requests.post('http://0.0.0.0:5000/start-job', json=task)
    assert resp.status_code == 200

for i in range(NUM):
    request_id = 'test-request'+str(i+offset)
    logging.info('Checking results of %s', request_id)
    requests.get('http://0.0.0.0:5000/jobs/{}/wait_ready'.format(request_id))
    output = json.loads(minio.get_object(request_id, output_url).read().decode())
    minio.stat_object(request_id, 'done')
    try:
        minio.stat_object(request_id, 'error')
        assert False
    except MinioError:
        pass
    assert output['key'] == [i]
    assert output['output'] == [i*2]
