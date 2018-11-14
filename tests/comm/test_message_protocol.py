import pytest
from shepherd.comm import *
from shepherd.errors.comm import MessageError, UnexpectedMessageTypeError


def test_send_rcv(dealer_socket, router_socket, message: Message):
    Messenger.send(dealer_socket, message)
    received = Messenger.recv(router_socket)
    assert message.job_id == received.job_id

    for key in message._data.keys():
        if key != 'identity':
            assert message._data[key] == received._data[key]


def test_send_rcv_reply_rcv(dealer_socket, router_socket, message: Message):
    Messenger.send(dealer_socket, message)
    tmp = Messenger.recv(router_socket)
    Messenger.send(router_socket, DoneMessage(dict(job_id=tmp.job_id)), tmp)
    received = Messenger.recv(dealer_socket)
    assert message.job_id == received.job_id


def test_wrong_type(dealer_socket):
    with pytest.raises(TypeError):
        Messenger.send(dealer_socket, 'jadyda')


def test_bad_socket(bad_socket):
    with pytest.raises(MessageError):
        Messenger.recv(bad_socket)
    with pytest.raises(MessageError):
        bad_socket.close()
        Messenger.send(bad_socket, DoneMessage(dict(job_id='')))


def test_unexpected(dealer_socket, router_socket):
    Messenger.send(dealer_socket, DoneMessage(dict(job_id='aaaa')))
    with pytest.raises(UnexpectedMessageTypeError):
        Messenger.recv(router_socket, [InputMessage])
