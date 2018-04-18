import json
import pytest
import os.path as path

import gevent
import subprocess

from cxworker.constants import OUTPUT_DIR, DEFAULT_OUTPUT_FILE
from cxworker.runner import *
from cxworker.runner.runner_entry_point import run
from cxworker.comm import *


def test_to_json_serializable(json_data):
    original, serializable = json_data
    assert serializable == to_json_serializable(original)
    with pytest.raises(ValueError):
        to_json_serializable(None)
    with pytest.raises(ValueError):
        to_json_serializable(gevent)


def test_json_runner(job, feeding_socket, runner_setup):
    socket, port = feeding_socket
    job_id, job_dir = job

    version, stream, expected = runner_setup
    config_path = path.join('examples', 'docker', 'cxflow_example', 'cxflow-test', version)
    runner = JSONRunner(config_path, port, stream)
    greenlet = gevent.spawn(runner.process_all)
    Messenger.send(socket, InputMessage(dict(job_id=job_id, io_data_root=job_dir)))
    Messenger.recv(socket, [DoneMessage])
    greenlet.kill()
    output = json.load(open(path.join(job_dir, job_id, OUTPUT_DIR, DEFAULT_OUTPUT_FILE)))

    assert output == {'key': [42], 'output': [expected]}


def test_json_runner_exception(job, feeding_socket):
    socket, port = feeding_socket
    job_id, job_dir = job

    config_path = path.join('examples', 'docker', 'cxflow_example', 'cxflow-test', 'latest')
    runner = JSONRunner(config_path, port, 'does_not_exist')
    greenlet = gevent.spawn(runner.process_all)
    Messenger.send(socket, InputMessage(dict(job_id=job_id, io_data_root=job_dir)))
    error = Messenger.recv(socket, [ErrorMessage])
    greenlet.kill()

    assert error.short_error == 'AttributeError: \'DummyDataset\' object has no attribute \'does_not_exist_stream\''


def start_cli(command, mocker):
    return subprocess.Popen(command)


def start_greenlet(command, mocker):
    mocker.patch('sys.argv', command)
    return gevent.spawn(run)


@pytest.mark.parametrize('start', (start_cli, start_greenlet))
def test_runner(job, feeding_socket, runner_setup, mocker, start):  # for coverage reporting
    socket, port = feeding_socket
    job_id, job_dir = job
    version, stream, expected = runner_setup
    base_config_path = path.join('examples', 'docker', 'cxflow_example', 'cxflow-test', version)

    # test both config by dir and config by file
    for config_path in [base_config_path, path.join(base_config_path, 'config.yaml')]:
        command = ['cxworker-runner', '-p', str(port), '-s', stream, config_path]
        handle = start(command, mocker)
        Messenger.send(socket, InputMessage(dict(job_id=job_id, io_data_root=job_dir)))
        Messenger.recv(socket, [DoneMessage])
        handle.kill()
        output = json.load(open(path.join(job_dir, job_id, OUTPUT_DIR, DEFAULT_OUTPUT_FILE)))
        assert output['output'] == [expected]


def test_runner_configuration(mocker):
    config_path = path.join('examples', 'docker', 'cxflow_example', 'cxflow-test', 'test')
    mocker.patch('sys.argv', ['cxworker-runner', '-p', '8888', config_path])
    with pytest.raises(ModuleNotFoundError):
        run()  # runner is configured to a non-existent module; thus, we expect a failure
