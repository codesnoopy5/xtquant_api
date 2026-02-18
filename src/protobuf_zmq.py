import zmq
from typing import Type, TypeVar
from google.protobuf.message import Message

T = TypeVar('T', bound=Message)

class SocketType:
    PUB = zmq.PUB
    SUB = zmq.SUB
    PUSH = zmq.PUSH
    PULL = zmq.PULL

class ProtobufZmqWrapper:
    def __init__(self, socket_type: int):
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(socket_type)
        self.socket_type = socket_type

    def bind(self, endpoint: str):
        self.socket.bind(endpoint)

    def connect(self, endpoint: str):
        self.socket.connect(endpoint)

    def send(self, msg: Message, non_blocking: bool = False) -> bool:
        try:
            flags = zmq.NOBLOCK if non_blocking else 0
            return self.socket.send(msg.SerializeToString(), flags=flags)
        except zmq.ZMQError:
            return False

    def recv(self, msg_type: Type[T], non_blocking: bool = False) -> T:
        try:
            flags = zmq.NOBLOCK if non_blocking else 0
            data = self.socket.recv(flags=flags)
            msg = msg_type()
            msg.ParseFromString(data)
            return msg
        except zmq.ZMQError:
            return None

    def set_subscription(self, topic: str = ""):
        if self.socket_type == zmq.SUB:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)

    def __del__(self):
        self.socket.close()
        self.ctx.term()
