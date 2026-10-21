"""Validated JSON-to-Protobuf bridge for the Yide C++ trading adapter.

This module has no broker/QMT imports, so its behavior can be tested offline.
An "accepted" response means queued, never executed or filled.
"""
import math
import re


def enqueue_volume_order(data, buy_queue, sell_queue, push_socket, push_lock, message_type):
    stock_code = data.get("stock_code")
    how_many = data.get("how_many")
    price = data.get("price")
    order_type = data.get("order_type")

    if not isinstance(stock_code, str) or re.fullmatch(r"[0-9]{6}", stock_code) is None:
        raise ValueError("stock_code must be exactly six digits")
    if type(how_many) is not int or not (0 < how_many <= 2147483647):
        raise ValueError("how_many must be a positive 32-bit share count")
    if type(price) not in (int, float) or not math.isfinite(price) or price <= 0:
        raise ValueError("price must be a finite positive number")
    if order_type not in ("buy", "sell"):
        raise ValueError("order_type must be buy or sell")

    queue = buy_queue if order_type == "buy" else sell_queue
    order = message_type()
    order.stock_code = stock_code
    order.how_many = how_many
    order.price = float(price)
    order.order_type = order_type
    order.place_type = "vol"

    # Same idempotency key as the existing /place_order/vol Protobuf endpoint.
    key = "placevol_" + stock_code
    with push_lock:
        if not queue.check_and_add(key):
            return {"status": "rejected", "msg": "duplicate order within cooldown"}
        # This only enqueues a request. It does NOT confirm broker acceptance or a fill.
        push_socket.send(order.SerializeToString())
    return {"status": "accepted", "msg": "queued for execution; not a broker acknowledgement"}
