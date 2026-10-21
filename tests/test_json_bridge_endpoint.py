"""Offline endpoint contract tests: execute only extracted adapter handlers, never start QMT."""
import ast
import asyncio
import pathlib
import sys
import threading
import types
import unittest

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, StrictInt, ValidationError

SRC = pathlib.Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))
from json_volume_bridge import enqueue_volume_order


class FakeOrder:
    def __init__(self):
        self.stock_code = ""
        self.how_many = 0
        self.price = 0
        self.order_type = ""
        self.place_type = ""

    def SerializeToString(self):
        return ("%s|%d|%s|%s|%s" %
                (self.stock_code, self.how_many, self.price,
                 self.order_type, self.place_type)).encode("ascii")


class FakeQueue:
    def __init__(self):
        self.seen = set()

    def check_and_add(self, key):
        if key in self.seen:
            return False
        self.seen.add(key)
        return True


class FakeSocket:
    def __init__(self):
        self.messages = []
        self.fail = False

    def send(self, body):
        if self.fail:
            raise OSError("offline simulated send failure")
        self.messages.append(body)


class FakeQMTManager:
    fail = False

    @classmethod
    def get_instance(cls):
        return cls()

    def query_stock_asset_en(self):
        if self.fail:
            raise RuntimeError("simulated broker disconnect")
        return {"account_id": "SIMULATED_ACCOUNT", "cash": 100.0,
                "frozen_cash": 0.0, "total_asset": 100.0,
                "market_value": 0.0}


class EndpointContractTests(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.state.buy_queue = FakeQueue()
        app.state.sell_queue = FakeQueue()
        app.state.zmq_util = types.SimpleNamespace(
            order_push_socket=FakeSocket(), order_push_lock=threading.Lock()
        )
        FakeQMTManager.fail = False
        env = {
            "app": app, "BaseModel": BaseModel, "StrictInt": StrictInt,
            "HTTPException": HTTPException, "enqueue_volume_order": enqueue_volume_order,
            "lg": types.SimpleNamespace(PlaceOrder=FakeOrder),
            "QMTManager": FakeQMTManager, "log_msg": lambda message: None
        }
        tree = ast.parse((SRC / "qmt_api.py").read_text(encoding="utf-8"))
        desired = {"BridgeVolumeOrder", "bridge_order_volume", "bridge_account_info"}
        nodes = [n for n in tree.body if getattr(n, "name", None) in desired]
        self.assertEqual({n.name for n in nodes}, desired)
        selected = ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[]))
        exec(compile(selected, str(SRC / "qmt_api.py"), "exec"), env)
        self.app = app
        self.env = env

    def test_routes_and_ack_not_filled(self):
        paths = {r.path for r in self.app.routes}
        self.assertIn("/bridge/order/vol", paths)
        self.assertIn("/bridge/account/info", paths)
        order = self.env["BridgeVolumeOrder"](
            stock_code="600000", how_many=200, price=12.3, order_type="buy")
        ack = asyncio.run(self.env["bridge_order_volume"](order))
        self.assertEqual(ack["status"], "accepted")
        self.assertIn("not a broker acknowledgement", ack["msg"])
        self.assertEqual(len(self.app.state.zmq_util.order_push_socket.messages), 1)
        dupe = asyncio.run(self.env["bridge_order_volume"](order))
        self.assertEqual(dupe["status"], "rejected")

    def test_strict_volume_and_code(self):
        with self.assertRaises(ValidationError):
            self.env["BridgeVolumeOrder"](
                stock_code="600000", how_many=1.5, price=12.3, order_type="buy")
        malformed = self.env["BridgeVolumeOrder"](
            stock_code="SH.600000", how_many=100, price=12.3, order_type="buy")
        with self.assertRaises(HTTPException) as failure:
            asyncio.run(self.env["bridge_order_volume"](malformed))
        self.assertEqual(failure.exception.status_code, 422)

    def test_unavailable_queue_fails(self):
        self.app.state.zmq_util.order_push_socket.fail = True
        order = self.env["BridgeVolumeOrder"](
            stock_code="000001", how_many=100, price=12.3, order_type="sell")
        with self.assertRaises(HTTPException) as failure:
            asyncio.run(self.env["bridge_order_volume"](order))
        self.assertEqual(failure.exception.status_code, 503)

    def test_account_success_and_error(self):
        data = self.env["bridge_account_info"]()
        self.assertEqual(data["status"], "success")
        self.assertEqual(data["data"]["account_id"], "SIMULATED_ACCOUNT")
        FakeQMTManager.fail = True
        with self.assertRaises(HTTPException) as failure:
            self.env["bridge_account_info"]()
        self.assertEqual(failure.exception.status_code, 503)


if __name__ == "__main__":
    unittest.main()
