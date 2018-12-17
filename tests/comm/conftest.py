import pytest
import zmq.green as zmq

from shepherd.comm import InputMessage, DoneMessage, ErrorMessage, JobDoneNotifier


messages = (InputMessage(dict(job_id='test_job', io_data_root='/tmp')),
            DoneMessage(dict(job_id='done_job')),
            ErrorMessage(dict(job_id='test_job', short_error='short err', long_error='it was really bad')))


@pytest.fixture(params=messages)
def message(request):
    yield request.param


@pytest.fixture()
def dealer_socket():
    sock = zmq.Context.instance().socket(zmq.DEALER)
    sock.bind('inproc://protocol')
    yield sock
    sock.disconnect('inproc://protocol')
    sock.close()


@pytest.fixture()
def router_socket():
    sock = zmq.Context.instance().socket(zmq.ROUTER)
    sock.setsockopt(zmq.IDENTITY, b'router')
    sock.connect('inproc://protocol')
    yield sock
    sock.disconnect('inproc://protocol')
    sock.close()


@pytest.fixture()
def bad_socket():
    sock = zmq.Context.instance().socket(zmq.REQ)
    # socket is not connected
    yield sock


@pytest.fixture()
def notifier():
    notifier = JobDoneNotifier()
    yield notifier
    notifier.close()
