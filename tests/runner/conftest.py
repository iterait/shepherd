import pytest
import zmq.green as zmq
import os
import json
import os.path as path
import numpy as np

original_data = [{'a': np.array([1, 2, 3])}, [np.array(1), np.array(2)]]
json_rerializable = [{'a': [1, 2, 3]}, [1, 2]]


@pytest.fixture(params=zip(original_data, json_rerializable))
def json_data(request):
    yield request.param


@pytest.fixture()
def feeding_socket():
    sock = zmq.Context.instance().socket(zmq.DEALER)
    port = sock.bind_to_random_port('tcp://0.0.0.0')
    yield sock, port
    sock.close()


@pytest.fixture(params=['first-job', 'second-job'])
def job(request, tmpdir_factory):
    dir_ = tmpdir_factory.mktemp('data')
    job_id = request.param
    input_dir = path.join(dir_, job_id, 'inputs')
    os.makedirs(input_dir)
    os.makedirs(path.join(dir_, job_id, 'outputs'))
    json.dump({'key': [42]}, open(path.join(input_dir, 'input.json'), 'w'))
    yield job_id, str(dir_)


@pytest.fixture(params=[('latest', 'predict', 42*2), ('production', 'production', 42*10*2)])
def runner_setup(request):
    yield request.param
