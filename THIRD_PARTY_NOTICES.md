# 第三方组件与授权范围 / Third-Party Notices

本仓库 `LICENSE` 中的 MIT 许可仅适用于作者有权授权的原创代码和文档。

- **QMT / MiniQMT / xtquant**：券商交易端、SDK 和账户授权均由其原提供方管理，仓库未授予相关软件的使用权。
- **qmt_trader**：`src/qmt_api.py` 引用外部 `qmt_trader.qmt_trader`，仓库未收录该模块；维护者及使用者需自行核实其来源、许可与部署依赖。
- **Protobuf**：`src/little_goal_pb2.py`、`src/pb/little_goal_pb2.py` 和 `src/pb/little_goal.pb.*` 属于生成文件。请保留文件自带的生成声明；Protobuf 编译器与运行时遵循其各自许可。
- **Python 依赖**：FastAPI、Pydantic、Uvicorn、pyzmq/ZeroMQ、protobuf、numexpr、pandas 等依赖遵守各自独立的开源条款；发布打包程序时应额外检查实际纳入的依赖。

本说明不是全面的软件物料清单或法律意见。在公开发布含第三方文件的压缩包、安装包前，应复核这些文件与依赖的具体许可证和版权通知。
