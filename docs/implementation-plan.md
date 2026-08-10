# ft-shadow-data-plane 最小实施计划

状态：实施基准
日期：2026-08-10
原则：边缘只记录事实，中心解释事实；宁可显式 gap，也不静默丢数。

## 1. 目标与边界

建立独立公开仓库 `50829/ft-shadow-data-plane`，在东京边缘机采集 Binance USD-M
公共数据，由校园 login node 定期拉取，再通过 Slurm 校验、规范化和重建 L2。

```text
Edge       记录“收到了什么”
Central    判断“哪些数据有效、市场发生了什么”
Experiment 使用已验证的数据生成特征和模型
```

仓库使用 Python 3.12 和 GPL-3.0，只包含三个包：

- `contracts`：raw、manifest、gap 和 control 数据契约；
- `edge`：公共行情采集、分块落盘、spool 和 SFTP 发布目录；
- `central`：SFTP 拉取、universe selector、完整性验证、typed events、L2 校验和 quality ledger。

旧 `ft-shadow` 的代码和数据不迁移，也不作为运行依赖。实验特征、标签、回测、模型和
交易执行不属于本仓库。

## 2. 采集范围与 universe

最多采集 60 个 Binance USD-M 合约。文档统一称为 instrument，交易所原始代码保存为
`exchange_symbol`，例如 `BTCUSDT`。

正式持续采集：

- WSS：`depth@100ms`、完整 `bookTicker`、`aggTrade`、`markPrice@1s`、
  `forceOrder`、`contractInfo`；
- REST：每个已选合约每 30 秒采集一次 OI；
- L2：每次 depth 连接建立或失效后获取 1000 档 REST snapshot；
- discovery：每天一次全市场 `exchangeInfo` 和 ticker/成交额，只用于选择采集对象。

不采集 kline WSS。K 线从交易重建，REST kline 只用于审计。

D0 临时双收一个完整 UTC 日的 `trade/aggTrade` 和普通/RPI depth，输出审计报告。正式
默认仍为 `aggTrade` 且关闭 RPI；D0 不自动修改默认合同。

中心固定 40–50 个正式实验成员，边缘采集池上限 60，并保留少量排名边界和新上市观察
位置。正常 universe 更新在每天 `00:00 UTC` 生效，最多替换 5 个，成员最短停留 48 小时；
新上市合约可立即进入专用观察位。全市场 discovery 数据不作为微观结构实验数据。

首次 selector 固定 50 个 core、5 个 boundary 和 5 个最新 probe，并从同一决策生成嵌套的
20、40、50、60 canary 名单。`CANARY_SCALE` 只允许按这些阶段增加成员；DAILY 的 5 个变更
限制只在 canary 完成后的稳态运行使用。每次 selector 决策必须保存原始 discovery hash、
规则、排名、排除原因和最终 universe hash。

60-instrument final canary 通过 72 小时后，首次 DAILY control 移除 5 个 canary probe，进入
50 core + 5 boundary 的 steady pool；由此预留的 5 个空位才允许日内 `new_listing_probe`
逐个增加。canary 期间禁止使用 `new_listing_probe`。

## 3. RawEventV1

WSS raw envelope 只包含：

```text
schema_version
exchange_symbol
stream_type
collector_id
boot_id
segment_id
connection_id
receive_seq
app_receive_realtime_ns
app_receive_monotonic_ns
payload_bytes
```

`payload_bytes` 保存实际收到的 JSON 字节，不再存一份解析 payload。接收时间在 `recv()`
返回后、JSON 解析前采样，只表示应用观察时间。REST 记录另存请求时间、响应观察时间和
request ID。

不建立通用 `exchange_event_id`。中央按 stream 使用 Binance 原字段，例如 `a`、`t`、
`U/u/pu`。raw 永不去重，重连重叠产生的重复消息也完整保留。

## 4. Edge 数据流

- 初始目标机器：东京 Vultr、Ubuntu 24.04、1C1G、25GB SSD；这只是待验证配置。
- 按 instrument 固定分成 4 个公共 WSS 连接，市场级流使用独立连接；在 Binance 24 小时
  生命周期前主动换连接并短暂重叠。
- 数据进入按字节限制的内存队列。队列达到 hard limit 时关闭相关 WSS，使用独立预留
  控制日志写入 `INGEST_OVERLOAD_GAP`，排空后以新 segment 恢复。
- 磁盘达到 hard limit 时停止采集并记录 `STORAGE_EXHAUSTED_GAP`。禁止覆盖未 ACK 数据、
  静默丢弃、自动降采样或无限堆积内存。
- 使用三个 writer：`depth`、`trades_market`、`metadata`。writer 与 WSS 连接分片无绑定。
- raw 使用 Parquet、Zstd level 1。chunk 最长 60 秒，并受大小和事件数上限约束。
- UTC 跨日、进程重启、schema/data contract 变化、universe 生效时强制切 chunk。

Chunk 状态只有：

```text
WRITING -> READY -> ACKED
```

每个 READY chunk 发布一个 sidecar manifest，包含 chunk ID、SHA-256、字节数、事件数、
接收时间范围及 contract/universe hash。Parquet 完成 fsync 和原子 rename 后才发布
manifest；puller 只扫描 manifest。edge 只删除已收到精确 `(chunk_id, sha256)` ACK 的文件。

## 5. 传输与中心处理

校园 login node 每分钟由 cron 主动 SFTP 到 edge：

1. 下载到 `.partial`；
2. 验证大小和 SHA-256；
3. 对文件和目录 fsync；
4. 原子 rename 到 persistent raw；
5. 向 edge 写入精确 ACK。

控制配置和 ACK 使用同一个受限 SSH/SFTP 账户。第一版依赖 SSH 身份认证和内容 hash，
不增加额外签名或控制服务。

edge 在 UTC 日结束后发布 `SEALED` day manifest。中心只有在 manifest 声明的全部
`(chunk_id, sha256)` 都已 durable 存在时，才提交 Slurm 处理。Day 状态只有：

```text
OPEN -> SEALED -> PROCESSED | FAILED
```

中央处理执行：

- schema、hash、时间和序列校验；
- 按各 stream 自身标识进行逻辑去重；
- 生成可由 raw 重建的 typed events；
- 重放 L2，生成有效区间和 gap ledger；
- 不为每个 depth 更新保存完整 1000 档快照。

L2 状态只有：

```text
UNANCHORED -> VALID -> GAPPED -> VALID
```

每个新连接必须独立完成 snapshot bridge。首个 diff 覆盖 snapshot 的 `lastUpdateId`，后续
要求 `pu == previous_u`。新连接进入 `VALID` 后才能替换旧连接；禁止跨连接拼接数据来
隐藏 gap。该数据明确称为 `public_l2_1000_anchored`，不宣称完整市场深度。

## 6. 容量、保留与告警

edge spool 初始上限 10GiB，文件系统至少保留 5GiB。生产容量由 60-instrument、72 小时
canary 决定：

```text
required_spool = 1.5 * canary期间最大滚动6小时压缩数据量
```

若 25GB SSD 无法同时满足 spool 和最小空闲要求，则扩容磁盘。若 1C1G 无法满足保真度，
则升级到 2C2G，不减少数据类型或采样频率。

中心 raw 默认保留 90 天；被正式实验 release 引用的 raw 不自动删除：

```text
required_quota =
retention_days * measured_daily_storage
* retained_representation_multiplier * 1.3
```

监控 CPU、CPU steal、RSS、Python/Arrow 内存、queue bytes、event-loop lag、writer 吞吐、
fsync、磁盘、SFTP 积压、重连、时钟质量和 gap。停采、hash 错误、时钟失效或拉取长期
积压时发送邮件。

## 7. 必要验证与上线顺序

只实现能够防止数据被静默破坏的测试：

- 每种正式 stream 至少一个 parser fixture；
- L2 snapshot bridge、重复 diff、`pu` 断裂和重连重叠；
- queue/disk hard limit 必须产生 gap，且不得删除未 ACK chunk；
- partial 下载、hash 错误、ACK 丢失和重复拉取保持幂等；
- UTC 跨日和 universe 在 `00:00 UTC` 生效时正确切 chunk。

不上没有行为价值的 mock、重复单元测试或实现细节测试。

Canary 按 20 -> 40 -> 50 -> 60 推进；每级至少运行完整 24 小时，60 个 instrument 最终
运行 72 小时。验收要求：无静默丢失、所有缺失都有 gap、无 OOM、队列不触及 hard
limit、event-loop lag p99 小于 100ms、CPU 和内存持续低于机器容量 80%，且 spool 公式成立。

校园管理员允许 login-node cron 每分钟执行轻量 SFTP pull 是生产上线硬前置。若不允许，
传输层改用对象存储，不在 login node 偷跑常驻 daemon。

## 8. 非目标

第一版不实现：通用事件身份框架、复杂 dataset revision、raw compaction、DuckDB 服务、
消息队列、对象存储、热升级、零停机、多副本容灾、SBOM、镜像签名、build provenance、
控制文件数字签名、实验特征、标签、回测和模型。

代码审查时应主动删除未被上述数据流、故障语义或验收条件使用的抽象、配置和测试。
