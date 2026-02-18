# ---------- zmq_util.py （独立文件）----------
import zmq
import threading

class ZMQUtil:
    _lock = threading.Lock()
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._init_zmq()
        return cls._instance

    def _init_zmq(self):
        self.order_context = zmq.Context()
        self.feedback_context = zmq.Context()

        # PUSH-PULL（inproc）
        self.order_push_socket = self.order_context.socket(zmq.PUSH)
        self.order_push_socket.bind("inproc://order_process")

        self.order_pull_socket = self.order_context.socket(zmq.PULL)
        self.order_pull_socket.connect("inproc://order_process")

        # PUB-SUB（tcp）
        self.feedback_pub_socket = self.feedback_context.socket(zmq.PUB)
        self.feedback_pub_socket.bind("tcp://localhost:5555")

        # 全局锁
        self.feedback_pub_lock = threading.Lock()
        self.order_push_lock = threading.Lock()
        self.order_pull_lock = threading.Lock()

    def shutdown(self):
        """释放 ZMQ 资源"""
        self.order_push_socket.close()
        self.order_pull_socket.close()
        self.feedback_pub_socket.close()
        self.order_context.term()
        self.feedback_context.term()



