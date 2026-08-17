# ft-shadow-data-plane

Binance USD-M 正式数据采集与重建流水线。v0.3.3 从 generation 1 直接采集 60 个合约：
50 core、5 boundary、5 probe。

```text
Binance -> Vultr collector -> Parquet/Zstd ready/
        -> restricted rsync over SSH -> 107 data/raw
        -> Slurm -> 107 data/derived
        -> ACK -> Vultr spool GC
```

Vultr 负责采集、完整 UTC 日流动性证据、排名和增量换币。107 只负责每分钟短时拉取、持久化校验、
ACK 和 Slurm 处理，不参与选币。成员不变的 UTC 日切不会停止数据源；替换一个币只在线更新
这个币涉及的订阅和 OI 任务，其余 59 个币保持在线。

正式名单证据见 [generation 1 决策](docs/bootstrap-liquidity-decision-2026-08-12.md)，规则、
边界语义和性能标准见 [实施合同](docs/implementation-plan.md)。部署入口：

- [Vultr 正式采集部署](deploy/vultr/README.md)
- [校园 107 拉取与处理部署](deploy/campus-107/README.md)
- [端到端部署顺序](docs/deployment.md)

本地验证：

```bash
uv sync --dev
uv run ruff check src tests
uv run mypy src
uv run pytest -q
```

`ready/` 中的文件只有在 107 校验 SHA-256、原子写入 `data/raw` 并回传 ACK 后才会由
Vultr 删除。任何无法证明完整性的时间段都必须用显式 gap 事件记录。

v0.3.1 在 107 派生处理中持久化每个 symbol 的日末 L2 checkpoint，次日先验证
`connection_id` 与 `pu` 连续性再继承盘口；transport/sequence gap 会阻断继承，恢复 snapshot
bridge 与 gap close 后才重新声明有效。正式 raw 合同和 generation 1 名单没有变化。

v0.3.2 把异常重连的 transport recovery 与 L2 snapshot readiness 分开：订阅 ACK 和每个受监控
stream 的首事件证明 raw 恢复后即关闭 transport gap，但每个币仍须独立完成 snapshot bridge 才能
重新进入 L2 `VALID`。正式 public 路由使用 4 个分片，降低单连接故障的币种范围和最慢重锚时间。
官方约束和定量依据见 [重连恢复调研](docs/binance-reconnect-recovery-research-2026-08-12.md)。

v0.3.3 将静默 stream 的恢复精确到 `(stream, symbol)`，控制 ACK 采用独立 10 秒 deadline；局部刷新
失败时只重建所属 route，并为主动中断的 route 完整登记 gap，不再让 180 秒 refresh timeout 终止
全部 60 币。历史 gap 内未收到的事件不能补回，边界与生产清点见
[v0.3.3 完整性调研](docs/v0.3.3-gap-integrity-recovery-research-2026-08-17.md)。
