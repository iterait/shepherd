import pytest
from shepherd.comm import *
from shepherd.errors.comm import MessageError, UnexpectedMessageTypeError


async def test_send_rcv(dealer_socket, router_socket, message: Message):
    await Messenger.send(dealer_socket, message)
    received = await Messenger.recv(router_socket)
    assert message.job_id == received.job_id

    for key in message._data.keys():
        if key != 'identity':
            assert message._data[key] == received._data[key]


async def test_send_rcv_reply_rcv(dealer_socket, router_socket, message: Message):
    await Messenger.send(dealer_socket, message)
    tmp = await Messenger.recv(router_socket)
    await Messenger.send(router_socket, DoneMessage(dict(job_id=tmp.job_id)), tmp)
    received = await Messenger.recv(dealer_socket)
    assert message.job_id == received.job_id


async def test_wrong_type(dealer_socket):
    with pytest.raises(TypeError):
        await Messenger.send(dealer_socket, 'jadyda')


@pytest.mark.skip
async def test_bad_socket(bad_socket):
    # TODO it looks like you don't get an error when calling .recv() on an unconnected socket anymore
    with pytest.raises(MessageError):
        await Messenger.recv(bad_socket)
    with pytest.raises(MessageError):
        bad_socket.close()
        await Messenger.send(bad_socket, DoneMessage(dict(job_id='')))


async def test_unexpected(dealer_socket, router_socket):
    await Messenger.send(dealer_socket, DoneMessage(dict(job_id='aaaa')))
    with pytest.raises(UnexpectedMessageTypeError):
        await Messenger.recv(router_socket, [InputMessage])
