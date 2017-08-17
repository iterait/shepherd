import time
import zmq

context = zmq.Context()
socket = context.socket(zmq.ROUTER)
socket.setsockopt(zmq.IDENTITY, b"container")
socket.bind("tcp://*:9999")

while True:
    identity, payload, *_ = socket.recv_multipart()
    print(payload)
    time.sleep(1)
    socket.send_multipart([identity, b"Thanks!"])
