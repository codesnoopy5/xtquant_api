"""Run without connecting to QMT: python -m unittest discover -s tests -v."""
import pathlib
import sys
import threading
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
from json_volume_bridge import enqueue_volume_order


class FakeOrder:
    def __init__(self):
        self.stock_code = None
        self.how_many = None
        self.price = None
        self.order_type = None
        self.place_type = None

    def SerializeToString(self):
        return ("%s|%s|%s|%s|%s" %
                (self.stock_code, self.how_many, self.price,
                 self.order_type, self.place_type)).encode("ascii")


class FakeQueue:
    def __init__(self, allow=True):
        self.allow = allow
        self.keys = []

    def check_and_add(self, key):
        self.keys.append(key)
        return self.allow


class FakeSocket:
    def __init__(self):
        self.payloads = []

    def send(self, data):
        self.payloads.append(data)


class VolumeBridgeTests(unittest.TestCase):
    def setUp(self):
        self.buy = FakeQueue()
        self.sell = FakeQueue()
        self.socket = FakeSocket()
        self.lock = threading.Lock()

    def submit(self, **changes):
        data = {"stock_code": "600000", "how_many": 500,
                "price": 10.2, "order_type": "buy"}
        data.update(changes)
        return enqueue_volume_order(data, self.buy, self.sell,
                                    self.socket, self.lock, FakeOrder)

    def test_buy_shares_not_percent(self):
        result = self.submit()
        self.assertEqual(result["status"], "accepted")
        self.assertEqual(self.buy.keys, ["placevol_600000"])
        self.assertEqual(self.socket.payloads, [b"600000|500|10.2|buy|vol"])

    def test_sell(self):
        self.submit(stock_code="000001", how_many=100, order_type="sell")
        self.assertEqual(self.sell.keys, ["placevol_000001"])
        self.assertEqual(self.socket.payloads, [b"000001|100|10.2|sell|vol"])

    def test_duplicate_is_not_enqueued(self):
        self.buy.allow = False
        self.assertEqual(self.submit()["status"], "rejected")
        self.assertEqual(self.socket.payloads, [])

    def test_invalid_inputs_fail_closed(self):
        cases = [
            {"stock_code": "SH.600000"}, {"stock_code": "6000"},
            {"how_many": 0}, {"how_many": -1}, {"how_many": 3.2},
            {"how_many": True}, {"how_many": 2147483648},
            {"price": 0}, {"price": float("nan")},
            {"price": float("inf")}, {"price": "10"},
            {"order_type": "other"}, {"order_type": "BUY"},
        ]
        for case in cases:
            with self.subTest(case=case):
                with self.assertRaises(ValueError):
                    self.submit(**case)
        self.assertEqual(self.socket.payloads, [])


if __name__ == "__main__":
    unittest.main()
