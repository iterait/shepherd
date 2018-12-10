import asyncio
import json
import pytest
import os
import re
import os.path as path

import subprocess
from threading import Thread

from shepherd.constants import OUTPUT_DIR, DEFAULT_OUTPUT_FILE
from shepherd.runner import *
from shepherd.runner.runner_entry_point import main
from shepherd.comm import *


def test_to_json_serializable(json_data):
    original, serializable = json_data
    assert serializable == to_json_serializable(original)
    with pytest.raises(ValueError):
        to_json_serializable(asyncio)


async def test_json_runner(job, feeding_socket, runner_setup, loop):
    socket, port = feeding_socket
    job_id, job_dir = job

    version, stream, expected = runner_setup
    config_path = path.join('examples', 'docker', 'emloop_example', 'emloop-test', version)
    runner = JSONRunner(config_path, port, stream)
    task = asyncio.create_task(runner.process_all())
    await Messenger.send(socket, InputMessage(dict(job_id=job_id, io_data_root=job_dir)))
    message: DoneMessage = await Messenger.recv(socket, [DoneMessage])
    task.cancel()
    output = json.load(open(path.join(job_dir, job_id, OUTPUT_DIR, DEFAULT_OUTPUT_FILE)))

    assert output == {'key': [42], 'output': [expected]}
    assert message.job_id == job_id


async def test_json_runner_exception(job, feeding_socket):
    socket, port = feeding_socket
    job_id, job_dir = job

    config_path = path.join('examples', 'docker', 'emloop_example', 'emloop-test', 'latest')
    runner = JSONRunner(config_path, port, 'does_not_exist')
    task = asyncio.create_task(runner.process_all())
    await Messenger.send(socket, InputMessage(dict(job_id=job_id, io_data_root=job_dir)))
    error = await Messenger.recv(socket, [ErrorMessage])
    task.cancel()

    assert error.message == 'AttributeError: \'DummyDataset\' object has no attribute \'does_not_exist_stream\''


def start_cli(command, mocker):
    handle = subprocess.Popen(command)
    return handle.kill


# TODO add some asyncio runner, using a background thread with a separate event loop might also be feasible
@pytest.mark.parametrize('start', (start_cli,))
async def test_runner(job, feeding_socket, runner_setup, mocker, start):  # for coverage reporting
    socket, port = feeding_socket
    job_id, job_dir = job
    version, stream, expected = runner_setup
    base_config_path = path.join('examples', 'docker', 'emloop_example', 'emloop-test', version)

    # test both config by dir and config by file
    for config_path in [base_config_path, path.join(base_config_path, 'config.yaml')]:
        command = ['shepherd-runner', '-p', str(port), '-s', stream, config_path]
        killswitch = start(command, mocker)
        await Messenger.send(socket, InputMessage(dict(job_id=job_id, io_data_root=job_dir)))
        await Messenger.recv(socket, [DoneMessage])
        killswitch()  # terminate the runner
        output = json.load(open(path.join(job_dir, job_id, OUTPUT_DIR, DEFAULT_OUTPUT_FILE)))
        assert output['output'] == [expected]


def test_runner_configuration(mocker):
    config_path = path.join('examples', 'docker', 'emloop_example', 'emloop-test', 'test')
    mocker.patch('sys.argv', ['shepherd-runner', '-p', '8888', config_path])
    with pytest.raises(ModuleNotFoundError):
        main()  # runner is configured to a non-existent module; thus, we expect a failure


def test_n_gpus(mocker):
    n_system_gpus = len([s for s in os.listdir("/dev") if re.search(r'nvidia[0-9]+', s) is not None])
    assert n_available_gpus() == n_system_gpus
    mocker.patch('os.environ', {'NVIDIA_VISIBLE_DEVICES': '0,3'})
    assert n_available_gpus() == 2
    mocker.patch('os.environ', {'CUDA_VISIBLE_DEVICES': '1'})
    assert n_available_gpus() == 1
    mocker.patch('os.environ', {'NVIDIA_VISIBLE_DEVICES': '0,3', 'CUDA_VISIBLE_DEVICES': ''})
    assert n_available_gpus() == 0
