# ft-shadow-data-plane

Binance USD-M 正式数据采集与重建流水线。v0.3.0 从 generation 1 直接采集 60 个合约：
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
