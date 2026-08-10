# ft-shadow-data-plane

Binance USD-M 公共行情数据平面。边缘采集器只保存 raw；中心节点完成校验、逻辑去重、
L2 重建和质量账本。任何无法证明连续的数据都表示为 gap，不会被 REST 回填伪装成连续。

实施合同见 [docs/implementation-plan.md](docs/implementation-plan.md)，部署前置与 canary 标准见
[docs/deployment.md](docs/deployment.md)。旧 `ft-shadow` 仓库不是运行依赖。

## 部署入口

部署文件按目标机器组织，不需要在多个目录之间拼装：

| 目标机器 | 职责 | 部署入口 |
| --- | --- | --- |
| Vultr 边缘机 | 采集 Binance 行情、保存并发布 raw chunk | [`deploy/vultr/README.md`](deploy/vultr/README.md) |
| 校园 107 | 定时拉取 raw，并向 Slurm 提交规范化和 L2 任务 | [`deploy/campus-107/README.md`](deploy/campus-107/README.md) |

## 数据范围

正式采集 `depth@100ms`、`bookTicker`、`aggTrade`、`markPrice@1s`、`forceOrder`、
`contractInfo`、30 秒 OI，以及 L2 REST snapshot。全市场 `exchangeInfo` 和 24 小时 ticker
每天低频采集，只用于生成 universe 决策，不作为微观结构证据。

边缘最多接收 60 个 instrument。校园端 selector 依据 discovery 数据生成可审计的成员决策，
边缘只验证和应用 versioned control。正常 DAILY 和 CANARY_SCALE control 必须在
`00:00 UTC` 生效；采集器若当时离线，会在恢复、启动 writer 前立即应用已经到期的最新 control。

## 开发

```bash
uv sync --dev
uv run pytest
uv run ruff check .
uv run mypy src
```

## 运行边缘采集器

```bash
cp deploy/vultr/edge.yaml.example /tmp/edge.yaml
# 本地运行时把 /tmp/edge.yaml 的 data_root 改为本地可写目录
uv run ft-data-edge --config /tmp/edge.yaml
```

生产部署从 [`deploy/vultr/README.md`](deploy/vultr/README.md) 开始；容器内 `data_root`
必须为 `/data`。初始 1C1G、25GB SSD 只是 canary 候选，不是容量结论。

## 首次 universe 与 canary

首次固定名单及原始 Binance discovery 快照位于
[`universe/bootstrap-2026-08-10T120017Z`](universe/bootstrap-2026-08-10T120017Z)。该名单固定
50 个 core、5 个 boundary 和 5 个最新上市 probe，并生成嵌套的 20、40、50、60 阶段。
Vultr 示例配置从 stage 20 启动。

完成一个阶段的验收后，为下一阶段生成 control：

```bash
uv run ft-data-control \
  --generation 2 \
  --effective-at 2026-08-12T00:00:00Z \
  --reason canary_scale \
  --members-file universe/bootstrap-2026-08-10T120017Z/stage-40.members.txt \
  --output universe-2.control.json
```

`CANARY_SCALE` 只允许按 `20 -> 40 -> 50 -> 60` 增加嵌套成员，不能在进入 DAILY 后使用。
stage 60 连续通过 72 小时后，使用一次 DAILY control 切换到同 bundle 的
`steady-55.members.txt`，释放 5 个新上市观察位。

## 更新 universe

先生成 control，再通过受限 SFTP 账户把文件放入 edge 的
`control/universe/inbox/*.control.json`：

```bash
uv run ft-data-control \
  --generation 2 \
  --effective-at 2026-08-11T00:00:00Z \
  --reason daily \
  --members BTCUSDT,ETHUSDT \
  --output universe-2.control.json
```

DAILY 更新最多增删各 5 个成员，已有成员至少停留 48 小时。`new_listing_probe` 可以在日内
增加观察位，但不能移除成员。无效 control 不会替换 last-known-good universe。

## 中心单次拉取

```bash
cp deploy/campus-107/central.yaml.example /tmp/central.yaml
uv run ft-data-pull --config /tmp/central.yaml
```

该命令设计为由校园 login node 的 cron 每分钟调用一次。正式部署前必须获得管理员许可。
校园端完整安装、验证和 Slurm 提交流程见
[`deploy/campus-107/README.md`](deploy/campus-107/README.md)。

## 中心处理

只有 `SEALED.json` 声明的全部 chunk 都已下载并通过 hash 校验后，该 UTC 日才可处理：

```bash
uv run ft-data-process normalize \
  --raw-root /persistent/ft-shadow-data-plane/raw \
  --derived-root /persistent/ft-shadow-data-plane/derived \
  --collector tokyo01 --date 2026-08-10

uv run ft-data-process l2 \
  --raw-root /persistent/ft-shadow-data-plane/raw \
  --derived-root /persistent/ft-shadow-data-plane/derived \
  --collector tokyo01 --date 2026-08-10 --symbol BTCUSDT
```

正式实验先用 `ft-data-release` pin 对应 `SEALED.json` hash。`ft-data-retain` 默认只 dry-run，
显式增加 `--apply` 才删除超过 90 天且未被 release 引用的 raw 日。

## 目录边界

```text
edge/ready/date=.../writer=.../         READY raw 与 sidecar
edge/ready/day-manifests/date=.../      不可变 SEALED 日清单
edge/control/                           ACK、universe 和未关闭 gap 状态

central/raw/collector=.../date=.../     权威 raw
central/raw/collector=.../day-manifests 完整性边界
central/derived/typed/                  可由 raw 重建的 typed events
central/derived/quality/                gap、clock 与 L2 validity
```

不要直接引用一个正在增长的目录作为实验数据集；实验引用边界是 collector、UTC date 和对应
`SEALED.json` 的 SHA-256。
