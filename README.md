# xtquant_api

**QMT / MiniQMT 交易执行与查询后端** | Python / FastAPI / Protobuf / ZeroMQ

为量化交易开发者提供一个本地 HTTP API 后端，连接券商 QMT / xtquant 交易环境，并与易得交易接口及公式 DLL 配合使用。适用于**开发与测试集成**；在真实账户使用前须完成接口兼容性和模拟盘验证。

> English: A local Python trading API bridge built with FastAPI, Protobuf and ZeroMQ for a QMT/xtquant-based execution workflow. QMT software, credentials and an external `qmt_trader` module are required.

## 相关项目

- **xtquant_api（本仓库）**：后端，负责接收委托、查询资金/持仓/成交与发布交易回报。
- [OpenTraderApi](https://github.com/codesnoopy5/OpenTraderApi)：易得交易界面 C++ 接口模块。
- [yd_dll](https://github.com/codesnoopy5/yd_dll)：易得公式/股池/预警集成 DLL。

简化链路：

```text
易得交易界面 ── OpenTraderApi ──┐
                              ├── 本机 HTTP/ZeroMQ ── xtquant_api ── QMT/xtquant ── 券商账户
易得公式/预警 ─── yd_dll ─────┘
```

不同功能可能使用不同的通道。运行时应先确认各组件的 URL、协议和字段保持一致。

## 源码和接口

| 路径 | 用途 |
| --- | --- |
| `src/qmt_api.py` | FastAPI 服务、QMT 连接、交易指令与查询 |
| `src/ZMQUtil.py` | 订单队列及成交/委托回报 ZeroMQ socket |
| `src/pb/little_goal.proto` | Protobuf 消息定义 |
| `src/little_goal_pb2.py` | 由 Protobuf 编译器生成的 Python 消息类 |
| `src/config.example.ini` / `src/config.example.json` | 可公开的配置模板；本地私有配置已从 Git 跟踪移除 |
| `src/readme.txt` | 旧版环境安装和 Nuitka 打包笔记 |

当前 `qmt_api.py` 声明的主要 HTTP 路由：

| 查询 | 交易及撤单 |
| --- | --- |
| `GET /account/info` | `POST /place_order/vol` |
| `GET /positions` | `POST /place_order/amount` |
| `POST /stock_positions` | `POST /place_order/percent` |
| `GET /orders` | `POST /cancel/order_id` |
| `GET /trades` | `POST /cancel/stock_scope` |
|  | `POST /entrusts` |

部分 POST 接口的请求/响应使用 **Protobuf 二进制**，并非 JSON；请参照 `src/pb/little_goal.proto` 与对应处理函数，不要假定所有端点都支持普通 JSON。默认 HTTP 监听为 `127.0.0.1:8000`，ZMQ 回报发布端为 `tcp://localhost:5555`，仅用于本机集成。

## 运行前提

- Windows 上可正常登录交易环境的 QMT / MiniQMT 客户端及对应券商账户权限。
- 与本地 QMT 版本兼容的 Python、`xtquant` SDK。
- 外部模块 **`qmt_trader.qmt_trader`**（由源码直接导入，**当前仓库并未收录**）；需要自行取得或实现适配模块。因此全新克隆本仓库不能保证直接启动。
- Python 包：FastAPI、Uvicorn、Pydantic、protobuf、pyzmq、numexpr 等；其他依赖取决于外部 `qmt_trader` 模块与 QMT 安装。旧版环境安装笔记见 `src/readme.txt`。

在 QMT Python 环境中安装所需第三方 Python 包（版本须与既有环境兼容）：

```powershell
python -m pip install fastapi uvicorn pydantic protobuf pyzmq numexpr
```

上述命令**不负责安装**券商提供的 `xtquant`、外部 `qmt_trader`、QMT 客户端或其他交易依赖。

## 配置和启动

1. 安装并启动 QMT / MiniQMT，登录模拟交易账户，确认本机 API 连接权限。
2. 复制 `src/config.example.ini` 为 `src/config.ini`（Git 已忽略此文件），再配置真实交易端目录、账户、会话及交易参数。已有本地配置可直接保留。以下为**示意配置**：

   ```ini
   [qmt]
   path = C:\path\to\QMT\userdata_mini
   session_id = 123456
   account = YOUR_SIMULATED_ACCOUNT
   account_type = STOCK
   is_slippage = true
   slippage = 0.01

   [trading]
   repeated_buy_cooldown_time = 5
   repeated_sell_cooldown_time = 5
   ```

3. 确认 `qmt_trader` 模块可导入，然后从 **`src` 工作目录**启动（代码按相对路径读取 `config.ini`）：

   ```powershell
   cd src
   python qmt_api.py
   ```

4. 在本机检查 `http://127.0.0.1:8000/docs` 是否有 API 文档；实际交易链路仍须以模拟委托及回报测试验证。

## 当前限制与风险

- 外部 `qmt_trader` 源码、安装与授权未随仓库发布；无法仅用这个仓库复现完整执行环境。
- 与 [OpenTraderApi](https://github.com/codesnoopy5/OpenTraderApi) 的部分示例请求路径和报文尚不一致，例如前端源码使用 `/order/percent`，而本后端声明 `/place_order/percent`；接入前需统一协议，**不要直接实盘**。
- `src/config.ini` 与 `src/config_*.json` 已从当前版本的 Git 跟踪中移除并添加忽略规则；仅提交不含真实账户的 `config.example.*` 模板。旧 Git 提交可能仍含历史账户标识，已暴露的凭据应进行轮换；移除当前版本文件不等于清除历史。
- 订单重复拦截、网络错误、撤单状态、持仓冻结、回报时序等需结合真实券商环境测试；不要以 HTTP 成功替代成交确认。
- 请勿将未经鉴权的 HTTP / ZeroMQ 服务开放到公共网络。

## License / 许可

本仓库原创部分采用 [MIT License](LICENSE)。QMT、xtquant、外部 `qmt_trader`、第三方 Python 包和 Protobuf 运行库分别遵守各自的许可协议；参见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。
