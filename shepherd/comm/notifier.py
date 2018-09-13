import logging
from typing import Callable

import zmq.green as zmq
from zmq.error import ZMQError
import gevent


class JobDoneNotifier:
    """
    Simple notifier build over green zmq PUB-SUB sockets.

    Notifier allows to spawn greenlets waiting for arbitrary conditions which are tested only when the notify method
    is called.
    """

    def __init__(self):
        """Create new JobDoneNotifier."""
        self._zmq_context = zmq.Context.instance()
        self._socket = self._zmq_context.socket(zmq.PUB)
        self._socket.bind("inproc://job-done")

    def notify(self) -> None:
        """Notify all the waiting greenlets that yet another job is finished."""
        self._socket.send(b'')

    def wait_for(self, test: Callable[[], bool]) -> None:
        """Wait until the test callback can be evaluated to true."""
        def wait_for_test():
            notification_listener = self._zmq_context.socket(zmq.SUB)
            notification_listener.setsockopt(zmq.SUBSCRIBE, b'')
            notification_listener.connect("inproc://job-done")
            while not test():
                notification_listener.recv()
            notification_listener.close()   # pragma: no cover
        gevent.spawn(wait_for_test).join()

    def close(self):
        """Close the underlying socket."""
        self._socket.close()
