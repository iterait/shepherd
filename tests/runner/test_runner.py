import sys
import json
import pytest
import os.path as path
from pytest_mock import mock

import gevent
import subprocess

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
    output = json.load(open(path.join(job_dir, job_id, 'outputs', 'output.json')))

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


def test_cli_runner(job, feeding_socket, runner_setup):
    socket, port = feeding_socket
    job_id, job_dir = job
    version, stream, expected = runner_setup
    config_path = path.join('examples', 'docker', 'cxflow_example', 'cxflow-test', version)

    proc = subprocess.Popen(['cxworker-runner', '-p', str(port), '-s', stream, config_path])
    Messenger.send(socket, InputMessage(dict(job_id=job_id, io_data_root=job_dir)))
    Messenger.recv(socket, [DoneMessage])
    proc.kill()
    output = json.load(open(path.join(job_dir, job_id, 'outputs', 'output.json')))

    assert output == {'key': [42], 'output': [expected]}


def test_cli_runner_from_python(job, feeding_socket, runner_setup, mocker):  # for coverage reporting
    socket, port = feeding_socket
    job_id, job_dir = job
    version, stream, expected = runner_setup
    config_path = path.join('examples', 'docker', 'cxflow_example', 'cxflow-test', version)
    mocker.patch('sys.argv', ['cxworker-runner', '-p', str(port), '-s', stream, config_path])
    greenlet = gevent.spawn(run)
    Messenger.send(socket, InputMessage(dict(job_id=job_id, io_data_root=job_dir)))
    Messenger.recv(socket, [DoneMessage])
    greenlet.kill()
    output = json.load(open(path.join(job_dir, job_id, 'outputs', 'output.json')))
    assert output == {'key': [42], 'output': [expected]}
