# -*- coding:utf-8 -*-
import os
os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"
import uvicorn
from ZMQUtil import ZMQUtil
from qmt_trader.qmt_trader import qmt_trader
from xtquant import xtconstant
from qmt_trader.qmt_trader import MyXtQuantTraderCallback
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import threading
import time
from collections import OrderedDict
from datetime import datetime
import little_goal_pb2 as lg
from qmt_trader.qmt_trader import QMTLogger
from qmt_trader.qmt_trader import log_msg
import configparser
import traceback

def load_config():
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')

    # 读取配置
    _config = {
        'buy_cooldown':config.getint('trading', 'repeated_buy_cooldown_time'),
        'sell_cooldown':config.getint('trading', 'repeated_sell_cooldown_time'),
        'path':config.get('qmt', 'path'),
        'session_id':config.getint('qmt', 'session_id'),
        'account':config.get('qmt', 'account'),
        'account_type':config.get('qmt', 'account_type'),
        'is_slippage':config.getboolean('qmt', 'is_slippage'),
        'slippage':config.getfloat('qmt', 'slippage')
    }
    return _config


def parse_protobuf(message_type):
    async def dependency(request:Request):
        data = await request.body()
        proto_msg = message_type()
        proto_msg.ParseFromString(data)
        return proto_msg
    return dependency

@asynccontextmanager
async def lifespan(app:FastAPI):
    """替代废弃的 on_event"""
    # ---------- 修改全局计时方法 ----------
    _config = load_config()
    app.state.config = _config
    BUY_COOLDOWN = _config['buy_cooldown']
    SELL_COOLDOWN = _config['sell_cooldown']

    app.state.buy_queue = DedupMessageQueue(cooldown=BUY_COOLDOWN)
    app.state.sell_queue = DedupMessageQueue(cooldown=SELL_COOLDOWN)
    # 初始化 ZMQ
    app.state.zmq_util = ZMQUtil()
    app.state.zmq_util._init_zmq()

    # 启动订单处理线程
    threading.Thread(
        target=process_queue_worker,
        daemon=True
    ).start()
    # 初始化 QMT
    QMTManager.get_instance()
    QMTLogger.get_instance()

    yield  # App 在此运行
    # 清理资源
    app.state.zmq_util.shutdown()


# 创建 FastAPI 实例
app = FastAPI(lifespan=lifespan)


class MyCallback(MyXtQuantTraderCallback):
    def on_stock_order(self, order):
        """
        委托回报推送
        :param order:XtOrder对象
        :return:
        """

        order_time = datetime.fromtimestamp(order.order_time)
        formatted_time = order_time.strftime("%Y-%m-%dT%H:%M:%S")

        order_data = lg.TradeFeedback()
        order_data.stock_code = order.stock_code[:-3]
        order_data.order_status = order.order_status
        order_data.order_id = order.order_id
        order_data.order_type = order.order_type
        order_data.traded_price = order.traded_price
        order_data.traded_vol = order.traded_volume
        order_data.order_time = formatted_time

        # print("成交回报:")
        order_str = order_data.SerializeToString()
        # print(order_data)
        with app.state.zmq_util.feedback_pub_lock:
            app.state.zmq_util.feedback_pub_socket.send(order_str)

        # log_msg("接收到成交回报", f"股票代码:{order_data.stock_code}, 订单编号:{order_data.order_id}，订单类型:{order_data.order_type}，成交价格:{order_data.traded_price}， 成交数量:{order_data.traded_vol }, 成交时间:{order_data.order_time}")


class DedupMessageQueue:
    def __init__(self, cooldown:int, max_size=1000):
        self.cooldown = cooldown
        self.cache = OrderedDict()  # Key:request tuple, Value:last_seen timestamp
        self.max_size = max_size

    def check_and_add(self, key:str) -> bool:
        current_time = time.monotonic()

        # ------ MODIFIED:Remove ALL expired entries during cleanup ------
        # Single expired entry removal for the given key
        if key in self.cache:
            if current_time - self.cache[key] > self.cooldown:
                self.cache[key] = current_time
                self.cache.move_to_end(key)  # Mark as most recent
            else:
                # Key exists and NOT expired -> valid duplicate
                return False
        else:
            self.cache[key] = current_time

        # Enforce max_size using LRU policy
        while len(self.cache) > max(0, self.max_size - 1):
            self.cache.popitem(last=False)

        return True


def process_queue_worker():
    """Process orders from queue"""
    while True:
        with app.state.zmq_util.order_pull_lock:
            raw_data  = app.state.zmq_util.order_pull_socket.recv()
            place_order = lg.PlaceOrder()
            place_order.ParseFromString(raw_data)

        # Execute order synchronously
        execute_order(place_order.stock_code, place_order.how_many, place_order.price, place_order.order_type, place_order.place_type)


# 替换 time.time() 为更稳定的 monotonic 计时器
if hasattr(time, 'monotonic'):
    get_current_time = time.monotonic  # Windows/Linux 都兼容
else:
    get_current_time = time.time       # 兼容旧版本

def dataframe_to_json(df):
    """
    将持仓DataFrame转换为JSON对象
    :param df:包含以下列的DataFrame:
        账号类型, 资金账号, 证券代码, 持仓数量, 可用数量, 平均建仓成本, 市值
    :return:JSON格式的字符串
    """
    # 将DataFrame转换为字典列表（每行一个字典）
    json = df.to_dict(orient='records')

    # 美化输出格式（可选）
    return json

# QMT交易器全局实例
class QMTManager:
    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        with cls._lock:
            if cls._instance is None:
                # 初始化参数 (需根据实际环境修改)

                cls._instance = qmt_trader(
                    path=app.state.config["path"],
                    session_id=app.state.config["session_id"],
                    account=app.state.config["account"],
                    account_type=app.state.config["account_type"],
                    is_slippage=app.state.config["is_slippage"],
                    slippage=app.state.config["slippage"]
                )
                log_msg(f"加载配置文件, miniqmt路径:{app.state.config['path']}, 账号:{app.state.config['account']}, 启用滑点:{app.state.config['is_slippage']}, 滑点:{app.state.config['slippage']}")
                cls._instance.connect(MyCallback())
            return cls._instance

# 指令模型定义
class OrderRequest(BaseModel):
    stock_code:str
    how_many:float
    price:float
    order_type:str = "buy"  # buy/sell

class EntrustsrRequest(BaseModel):
    stock_code:str
    trade_type:str
    data_type:str

class CancelOrderRequest(BaseModel):
    order_id:int
    stock_code:int

class StockOrderRequest(BaseModel):
    stock_code:str
    trade_type:str
    amount:int
    price:float

# API路由实现
@app.get("/account/info")
def get_account_info(request:Request):
    """查询账户资金信息"""
    accountInfoResponse = lg.AccountInfoResponse()
    try:
        trader = QMTManager.get_instance()
        asset_info = trader.query_stock_asset_en()
        accountInfoResponse.data.account_id = asset_info['account_id']
        accountInfoResponse.data.cash = round(asset_info['cash'],2)
        accountInfoResponse.data.frozen_cash = round(asset_info['frozen_cash'])
        accountInfoResponse.data.total_asset = round(asset_info['total_asset'])
        accountInfoResponse.data.market_value = round(asset_info['market_value'])
        accountInfoResponse.status = "success"
        log_msg(f"收到查询账户指令, 账号:{asset_info['account_id']}, 可用金额:{accountInfoResponse.data.cash}, 冻结金额:{accountInfoResponse.data.frozen_cash }, 总资产:{accountInfoResponse.data.total_asset}, 持仓市值:{accountInfoResponse.data.market_value}")

    except Exception as e:
        log_msg(str(e))
        accountInfoResponse.data.account_id = ''
        accountInfoResponse.data.cash = 0
        accountInfoResponse.data.frozen_cash = 0
        accountInfoResponse.data.total_asset = 0
        accountInfoResponse.data.market_value = 0
        accountInfoResponse.status = "success"
    return StreamingResponse(
        iter([accountInfoResponse.SerializeToString()]),
        media_type="application/protobuf"
    )

@app.get("/positions")
def get_positions():
    """查询账户持仓"""
    positionResponse = lg.PositionsResponse()
    log_msg(f"收到查询账户持仓指令")
    try:
        trader = QMTManager.get_instance()
        positions = trader.query_stock_positions_en()

        for position in positions.to_dict(orient='records'):
            pb_position = lg.Position()
            pb_position.account_id = position['account_id']
            pb_position.stock_code = position['stock_code']
            pb_position.vol = position['vol']
            pb_position.available_vol = position['available_vol']
            pb_position.avg_cost = round(position['avg_cost'], 2)
            positionResponse.positions.append(pb_position)
            # log_msg(f"账号:{position['account_id']}, 股票代码:{position['stock_code']}, 持仓数量:{position['vol']}, 可用数量:{position['available_vol']}, 平均成本:{pb_position.avg_cost}")
        positionResponse.status = "success"

    except Exception as e:
        log_msg(str(e))
        pb_position = lg.Position()
        pb_position.account_id = ''
        pb_position.stock_code = ''
        pb_position.vol = 0
        pb_position.available_vol = 0
        pb_position.avg_cost = 0
        positionResponse.positions.append(pb_position)
        positionResponse.status = "success"
    return StreamingResponse(
        iter([positionResponse.SerializeToString()]),
        media_type="application/protobuf"
    )

@app.post("/stock_positions")
async def get_stock_positionss(
        stock_positions:lg.StockPositions = Depends(parse_protobuf(lg.StockPositions))):

    positionResponse = lg.PositionsResponse()
    pb_position = lg.Position()
    """查询账户持仓"""
    try:
        trader = QMTManager.get_instance()
        log_msg(f"收到查询持股指令, 股票代码:{stock_positions.stock_code}")
        position = trader.query_stock_position_en(stock_positions.stock_code)
        if(position):
            pb_position.account_id = position['account_id']
            pb_position.stock_code = position['stock_code']
            pb_position.vol = position['vol']
            pb_position.available_vol = position['available_vol']
            pb_position.avg_cost = position['avg_cost']
            positionResponse.positions.append(pb_position)
            log_msg(f"查询到持股:账号:{position['account_id']}, 股票代码:{position['stock_code']}, 持仓数量:{position['vol']}, 可用数量:{position['available_vol']}, 平均成本:{round(position['avg_cost'],2)}")
            positionResponse.status = "success"
    except Exception as e:
        log_msg(f"股票代码:{stock_positions.stock_code} " + str(e))

        pb_position.account_id = ''
        pb_position.stock_code = ''
        pb_position.vol = 0
        pb_position.available_vol = 0
        pb_position.avg_cost = 0
        positionResponse.positions.append(pb_position)
        positionResponse.status = "success"

    return StreamingResponse(
        iter([positionResponse.SerializeToString()]),
        media_type="application/protobuf"
    )



@app.get("/orders")
def get_orders(cancelable_only:bool = False):
    """查询当日委托"""
    orderResponse = lg.OrderResponse()
    try:
        log_msg("收到查询当日委托指令")
        trader = QMTManager.get_instance()
        orders = trader.query_stock_orders_en()
        for order in orders.to_dict(orient='records'):
            pb_order = lg.Order()
            pb_order.account_id = order['account_id']
            pb_order.stock_code = order['stock_code']
            pb_order.order_id = order['order_id']

            formatted_time = order['order_time'].strftime("%Y-%m-%dT%H:%M:%S")
            pb_order.order_time = formatted_time
            pb_order.order_type = order['order_type']
            pb_order.order_vol = order['order_volume']
            pb_order.price_type = order['price_type']
            pb_order.price = order['price']
            pb_order.traded_vol = order['traded_volume']
            pb_order.order_status = order['order_status']
            orderResponse.orders.append(pb_order)
        orderResponse.status = "success"
        return StreamingResponse(
            iter([orderResponse.SerializeToString()]),
            media_type="application/protobuf"
        )
    except Exception as e:
        log_msg(str(e))
        orderResponse.status = "error"
    return StreamingResponse(
        iter([orderResponse.SerializeToString()]),
        media_type="application/protobuf"
    )

@app.get("/trades")
def get_trades():
    """获取当日成交"""
    tradeResponse = lg.TradeResponse()
    try:
        trader = QMTManager.get_instance()
        trades = trader.query_stock_trades_en()
        log_msg("收到查询当日成交指令")

        for trade in trades.to_dict(orient='records'):
            pb_trade = lg.Trade()
            pb_trade.account_id = trade['account_id']
            pb_trade.stock_code = trade['stock_code']
            pb_trade.order_type = trade['order_type']
            pb_trade.traded_id = trade['traded_id']
            formatted_time = trade['traded_time'].strftime("%Y-%m-%dT%H:%M:%S")
            pb_trade.traded_time = formatted_time
            pb_trade.traded_price = trade['traded_price']
            pb_trade.traded_vol = trade['traded_volume']
            tradeResponse.trades.append(pb_trade)
        tradeResponse.status = "success"
    except Exception as e:
        log_msg(str(e))
        tradeResponse.status = "error"
    return StreamingResponse(
        iter([tradeResponse.SerializeToString()]),
        media_type="application/protobuf"
    )


@app.post("/place_order/vol")
async def place_vol_order(
        place_order:lg.PlaceOrder = Depends(parse_protobuf(lg.PlaceOrder))
):
    """按数量下单"""
    placeOrderResponse = lg.PlaceOrderResponse()
    try:
        request_key = f'placevol_{place_order.stock_code}'

        # Select appropriate queue based on trade type
        queue = app.state.buy_queue if place_order.order_type == "buy" else app.state.sell_queue


        place_order.place_type = 'vol'
        place_order.price = round(place_order.price, 2)
        if queue.check_and_add(request_key):
            with app.state.zmq_util.order_push_lock:
                app.state.zmq_util.order_push_socket.send(place_order.SerializeToString())
            msg = f"指令已提交，下单类型:{place_order.place_type}, 股票代码:{place_order.stock_code}, 委托数量:{place_order.how_many}, 委托价格:{place_order.price}, 下单类型:{place_order.order_type}"
            log_msg(f"收到下单指令, " + msg)
            placeOrderResponse.status = "accepted"
            placeOrderResponse.msg = msg
        else:
            msg = f"股票代码:{place_order.stock_code}， 委托数量:{place_order.how_many}， 委托价格:{place_order.price}, 下单类型:{place_order.order_type}"
            log_msg("拒绝重复下单, " + msg)
            placeOrderResponse.status = "rejected"
            placeOrderResponse.msg = msg
    except Exception as e:
        log_msg(str(e))
        placeOrderResponse.status = "error"
    return StreamingResponse(
        iter([placeOrderResponse.SerializeToString()]),
        media_type="application/protobuf"
    )

@app.post("/place_order/percent")
async def place_vol_order(
        place_order:lg.PlaceOrder = Depends(parse_protobuf(lg.PlaceOrder))
):
    """按数量（或百分比）下单"""
    placeOrderResponse = lg.PlaceOrderResponse()
    try:
        request_key = f'placepercent_{place_order.stock_code}'
        place_order.price = round(place_order.price, 2)
        # Select appropriate queue based on trade type
        queue = app.state.buy_queue if place_order.order_type == "buy" else app.state.sell_queue
        place_order.place_type = 'percent'
        if queue.check_and_add(request_key):
            with app.state.zmq_util.order_push_lock:
                app.state.zmq_util.order_push_socket.send(place_order.SerializeToString())
            msg = f"指令已提交，下单类型:{place_order.place_type}, 股票代码:{place_order.stock_code}, 委托百分比:{place_order.how_many}, 委托价格:{place_order.price}, 下单类型:{place_order.order_type}"

            placeOrderResponse.status = "accepted"
            placeOrderResponse.msg = msg
            log_msg("收到下单指令, " + msg)
        else:
            msg = f"股票代码:{place_order.stock_code}， 委托百分比:{place_order.how_many}， 委托价格:{place_order.price}, 下单类型:{place_order.order_type}"

            placeOrderResponse.status = "rejected"
            placeOrderResponse.msg = msg
            log_msg("拒绝重复下单指令, " + msg)
    except Exception as e:
        log_msg(str(e))
        placeOrderResponse.status = "error"
    return StreamingResponse(
        iter([placeOrderResponse.SerializeToString()]),
        media_type="application/protobuf"
    )


@app.post("/place_order/amount")
async def place_amount_order(
        place_order:lg.PlaceOrder = Depends(parse_protobuf(lg.PlaceOrder))
):
    placeOrderResponse = lg.PlaceOrderResponse()
    """按金额下单"""
    try:
        amount = round(place_order.how_many, 2)
        request_key = f'placeamount_{place_order.stock_code}'
        queue = app.state.buy_queue if place_order.order_type == "buy" else app.state.sell_queue

        place_order.price = round(place_order.price,2)
        place_order.place_type = 'amount'
        if queue.check_and_add(request_key):
            with app.state.zmq_util.order_push_lock:
                app.state.zmq_util.order_push_socket.send(place_order.SerializeToString())
            msg = f"指令已提交, 下单类型:{place_order.place_type}, 股票代码:{place_order.stock_code}, 委托金额:{amount}, 委托价格:{place_order.price}, 下单类型:{place_order.order_type}"
            placeOrderResponse.status = "accepted"
            placeOrderResponse.msg = msg
            log_msg("收到下单指令, " + msg)
        else:
            msg = f"拒绝重复指令:股票代码:{place_order.stock_code}, 委托金额:{amount}, 委托价格:{place_order.price}, 下单类型:{place_order.order_type}"
            placeOrderResponse.status = "rejected"
            placeOrderResponse.msg = msg
            log_msg("拒绝重复下单, " + msg)

    except Exception as e:
        log_msg(str(e))
        placeOrderResponse.status = "error"
    return StreamingResponse(
        iter([placeOrderResponse.SerializeToString()]),
        media_type="application/protobuf"
    )


@app.post("/cancel/order_id")
async def cancel_by_order_id(
        cancel_order_id:lg.CancelOrderId = Depends(parse_protobuf(lg.CancelOrderId))):
    """按订单号撤单"""
    cancelOrderIdResponse = lg.CancelOrderIdResponse()
    try:

        trader = QMTManager.get_instance()
        msg = trader.cancel_order_stock_async(cancel_order_id.order_id)
        log_msg(f"收到按订单编号撤单指令, 订单编号:{cancel_order_id.order_id}")
        cancelOrderIdResponse.status = "success"
        cancelOrderIdResponse.msg = str(msg)
    except Exception as e:
        log_msg(str(e))
        cancelOrderIdResponse.status = "error"
    return StreamingResponse(
        iter([cancelOrderIdResponse.SerializeToString()]),
        media_type="application/protobuf"
    )

@app.post("/cancel/stock_scope")
async def cancel_by_stock_scope(
        cancel_scope:lg.CancelStockScope = Depends(parse_protobuf(lg.CancelStockScope))):
    """按股票代码撤单"""
    log_msg(f"收到按股票范围撤单指令, 撤单类型:{cancel_scope.order_type}, 股票代码:{cancel_scope.stock_code}")
    try:
        trader = QMTManager.get_instance()
        msg = ""
        if(cancel_scope.order_type == "buy"):
            # 撤买单
            if(cancel_scope.stock_code != "all"):
                # 撤单个股票
                msg = trader.one_click_cancel_order_buy_by_stock(cancel_scope.stock_code)
            else:
                # 撤所有股票
                msg = trader.one_click_cancel_order_buy()
        elif(cancel_scope.order_type == "sell"):
            # 撤卖单
            if (cancel_scope.stock_code != "all"):
                # 撤单个股票
                msg = trader.one_click_cancel_order_sell_by_stock(cancel_scope.stock_code)
            else:
                # 撤所有股票
                msg = trader.one_click_cancel_order_sell()
        elif (cancel_scope.order_type == "all"):
            # 买卖都撤
            if (cancel_scope.stock_code != "all"):
                # 撤单个股票
                msg = trader.one_click_cancel_order_by_stock(cancel_scope.stock_code)
            else:
                # 撤所有股票
                msg = trader.one_click_cancel_order_all()

        cancelStockScopeResponse = lg.CancelStockScopeResponse()
        cancelStockScopeResponse.status = "success"
        cancelStockScopeResponse.stock_code = cancel_scope.stock_code
        cancelStockScopeResponse.order_type = cancel_scope.order_type
        cancelStockScopeResponse.msg = str(msg)

    except Exception as e:
        log_msg(str(e))
        cancelStockScopeResponse = lg.CancelStockScopeResponse()
        cancelStockScopeResponse.status = "error"
    return StreamingResponse(
        iter([cancelStockScopeResponse.SerializeToString()]),
        media_type="application/protobuf"
    )


@app.post("/entrusts")
async def entrusts(
        entrusts:lg.Entrusts = Depends(parse_protobuf(lg.Entrusts))):
    """按股票代码查询当日委托"""
    entrustsResponse = lg.EntrustsResponse()
    try:
        trader = QMTManager.get_instance()
        result, _ = trader.get_check_not_trader_today_entrusts_en( stock=entrusts.stock_code, trade_type=entrusts.trade_type, data_type=entrusts.data_type)
        log_msg(f"收到按股票代码查询当日委托指令, 股票代码:{entrusts.stock_code}, 委托类型:{entrusts.trade_type}, 委托数量:{entrusts.data_type}")
        entrustsResponse.status = "success"
        entrustsResponse.result = result

    except Exception as e:
        log_msg(str(e))
        entrustsResponse.status = "error"
    return StreamingResponse(
        iter([entrustsResponse.SerializeToString()]),
        media_type="application/protobuf"
    )

@app.post("/today_entrusts_value")
async def entrusts(
        entrusts:lg.Entrusts = Depends(parse_protobuf(lg.Entrusts))):
    """按股票代码查询当日委托"""
    todayEntrustsValueResponse = lg.TodayEntrustsValueResponse()
    try:
        trader = QMTManager.get_instance()
        envalue, unvalue = trader.get_today_entrusts_amount(trade_type=entrusts.trade_type)
        log_msg(f"收到查询当日委托金额指令, 委托类型:{entrusts.trade_type}")
        todayEntrustsValueResponse.status = "success"
        todayEntrustsValueResponse.envalue = envalue
        todayEntrustsValueResponse.unvalue = unvalue

    except Exception as e:
        log_msg(str(e))
        todayEntrustsValueResponse.status = "error"
    return StreamingResponse(
        iter([todayEntrustsValueResponse.SerializeToString()]),
        media_type="application/protobuf"
    )


def execute_order(stock_code:str, how_many:float, price:float, order_type:str, place_type:str):
    """后台实际执行下单"""
    order_id = ""
    try:
        trader = QMTManager.get_instance()
        # current_time = datetime.now().strftime("%H:%M:%S.%f")  # %f 是微秒，切掉后3位保留毫秒
        place_type_cn = ''
        if place_type == 'amount':
            place_type_cn = '委托金额'
        elif place_type == 'vol':
            place_type_cn = '委托数量'
        else:
            place_type_cn = '委托百分比'
        price = round(price, 2)
        log_msg(f"开始下单, 股票代码:{stock_code}, {place_type_cn}:{how_many}, 委托价格:{price}, 订单类型:{order_type}, 下单类型:{place_type}")
        # 检查交易环境
        # if not trader.check_is_trader_date():
        #     raise Exception("非交易时间")

        stock_code = stock_code.replace("SH", "")
        stock_code = stock_code.replace("SZ", "")
        stock_code = stock_code.replace("BJ", "")
        stock_code = stock_code.replace(".", "")
        # 根据类型执行下单
        if place_type == "vol":
            vol = int(how_many)

            if (vol > 0):
                if order_type == "buy":
                    order_id = trader.order_stock_async(stock_code=stock_code, order_type=xtconstant.STOCK_BUY, order_volume=int(vol),
                                             price=price)
                    log_msg(f"完成下单, 股票代码:{stock_code}, order_volume:{int(vol)}, 委托价格:{price}, 订单类型:{order_type}, 下单类型:{place_type}")
                elif order_type == "sell":
                    order_id = trader.order_stock_async(stock_code=stock_code, order_type=xtconstant.STOCK_SELL, order_volume=int(vol),
                                             price=price)
                    log_msg(
                        f"完成下单, 股票代码:{stock_code}, order_volume:{int(vol)}, 委托价格:{price}, 订单类型:{order_type}, 下单类型:{place_type}")
        if place_type == "amount":
            amount = how_many
            if (amount > 0):
                if order_type == "buy":
                        _, trade_vol, price = trader.order_value(stock=stock_code, value=amount, price=price, trade_type='buy')
                        order_id = trader.order_stock_async(stock_code=stock_code, order_type=xtconstant.STOCK_BUY, order_volume=int(trade_vol), price=price)
                        log_msg(
                            f"完成下单, 股票代码:{stock_code}, order_volume:{int(trade_vol)}, 委托价格:{price}, 订单类型:{order_type}, 下单类型:{place_type}")

                elif order_type == "sell":
                        _, trade_vol, price = trader.order_value(stock=stock_code, value=amount, price=price, trade_type='sell')
                        order_id = trader.order_stock_async(stock_code=stock_code, order_type=xtconstant.STOCK_SELL, order_volume=int(trade_vol),
                                             price=price)
                        log_msg(
                            f"完成下单, 股票代码:{stock_code}, order_volume:{int(trade_vol)}, 委托价格:{price}, 订单类型:{order_type}, 下单类型:{place_type}")
        if place_type == "percent":
            percent = how_many
            if (percent >= 0 and percent <= 100):
                percent = round(percent / 100.0,2)
                if order_type == "buy":
                    _, trade_vol, price = trader.order_percent(stock=stock_code, percent=percent, price=price, trade_type='buy')
                    order_id = trader.order_stock_async(stock_code=stock_code, order_type=xtconstant.STOCK_BUY, order_volume=int(trade_vol), price=price)
                    log_msg(
                        f"完成下单, 股票代码:{stock_code}, order_volume:{int(trade_vol)}, 委托价格:{price}, 订单类型:{order_type}, 下单类型:{place_type}")
                elif order_type == "sell":
                    _, trade_vol, pricse = trader.sell_percent_volume(stock=stock_code, percent=percent, price=price)
                    order_id = trader.order_stock_async(stock_code=stock_code, order_type=xtconstant.STOCK_SELL, order_volume=int(trade_vol),
                                                 price=price)
                    log_msg(
                        f"完成下单, 股票代码:{stock_code}, order_volume:{int(trade_vol)}, 委托价格:{price}, 订单类型:{order_type}, 下单类型:{place_type}")
    except Exception as e:
        # 获取完整的堆栈信息字符串
        error_info = traceback.format_exc()
        # 将错误信息和堆栈一起记录
        log_msg(f"下单发生异常: {str(e)}\n完整堆栈如下:\n{error_info}")
    return order_id

if __name__ == "__main__":

    uvicorn.run(app, host="127.0.0.1", port=8000, workers=1, access_log=False, log_level="error")