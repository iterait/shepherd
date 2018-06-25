import pytest
import zmq.green as zmq
import os
import json
import os.path as path
import numpy as np

from cxworker.constants import INPUT_DIR, OUTPUT_DIR, DEFAULT_PAYLOAD_FILE

original_data = [{'a': np.array([1, 2, 3])}, [np.array(1), np.array(2)], (np.array(42), 'crazy', None)]
serializable_data = [{'a': [1, 2, 3]}, [1, 2], [42, 'crazy', None]]


@pytest.fixture(params=zip(original_data, serializable_data))
def json_data(request):
    yield request.param


@pytest.fixture()
def feeding_socket():
    sock = zmq.Context.instance().socket(zmq.DEALER)
    sock.connect('tcp://0.0.0.0:9009')
    yield sock, 9009
    sock.disconnect('tcp://0.0.0.0:9009')
    sock.close()


@pytest.fixture(params=['first-job', 'second-job'])
def job(request, tmpdir_factory):
    dir_ = tmpdir_factory.mktemp('data')
    job_id = request.param
    input_dir = path.join(dir_, job_id, INPUT_DIR)
    os.makedirs(input_dir)
    os.makedirs(path.join(dir_, job_id, OUTPUT_DIR))
    json.dump({'key': [42]}, open(path.join(input_dir, DEFAULT_PAYLOAD_FILE), 'w'))
    yield job_id, str(dir_)


@pytest.fixture(params=[('latest', 'predict', 42*2), ('production', 'production', 42*10*2)])
def runner_setup(request):
    yield request.param
