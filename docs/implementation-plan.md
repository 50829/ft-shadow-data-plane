# v0.2 正式采集实施合同

## 本阶段目标

本版本就是正式实验采集版本。新数据从空状态启动，不读取、转换或迁移 v0.1 的 universe
状态和 raw 数据。generation 1 一次启动全部 60 个合约；不存在分级扩容配置。

Vultr 是 universe 决策者和执行者。107 仅拉取 immutable raw chunk、完成哈希校验、回传
ACK，并把重计算提交给 Slurm。正式采集过程中不依赖 GitHub，也不依赖 107 回传选币决策。

## Universe 角色

- `core` 固定 50 个槽位，代表长期稳定样本；
- `boundary` 固定 5 个槽位，代表流动性排名边界；
- `probe` 固定 5 个槽位，代表最新上市的合格永续合约；
- 三个角色始终互斥，总数始终等于 60。

Vultr 每天 `23:50 UTC` 连续请求一次 `exchangeInfo`、一次全市场 24h ticker，再请求第二次
`exchangeInfo`。只有两次 `exchangeInfo` 都为 `TRADING` 的 USDT 保证金 USDT 报价永续合约
才合格。原始响应、时间和 SHA-256 都保存在本地 decision evidence 中。

## 自动轮换

候选角色每天 `00:00 UTC` 生效：

- 流动性使用最近 1 至 7 个完整日观测的 `quoteVolume` 均值；
- boundary 目标为非 core、非 probe 的 Top5，现有成员在 Top10 内可保留；
- probe 优先最新上市的合格合约；
- 正常情况下每天最多替换 2 个币，boundary 和 probe 各最多 1 个；
- candidate 成员至少停留 48 小时；
- 两次状态请求确认停止交易后，允许为恢复可采集性进行强制替换。

core 只在周一 `00:00 UTC` 评估：

- 至少 7 个完整日观测，合约年龄至少 30 天；
- 新成员必须进入 Top45；现有成员跌出 Top55 后才具备退出资格；
- core 成员至少停留 14 天；
- 每周最多替换 5 个 core；
- 已被两次状态请求确认停止交易的 core 可优先替换。

每次评估都写 evaluation。成员变化时写带 generation、角色、证据 hash、原因、
`effective_at` 和 `universe_hash` 的 decision。`automation_enabled: false` 可暂停自动决策；
手工 override 只能修改 boundary/probe，不能直接修改 core。

## 日切和 gap

无成员变化的 UTC 日切 rollover gap journal，并通过 writer barrier finalize 前一天所有 chunk 后
seal；它不停止或重建任何 Binance 连接，也不产生 `PLANNED_BOUNDARY_GAP`。writer barrier
由 ingest lock 串行化，因此不会丢弃边界上的事件。

有成员变化时先切换 writer 的 `universe_hash`，再通过现有连接发送
`UNSUBSCRIBE/SUBSCRIBE`。新增币完成订阅 ACK、L2 snapshot 和第一次 OI 后关闭 gap。gap 的
`exchange_symbols` 只包含集合差集，不包含未变化的币。因此 candidate 轮换不会让 50 个
core 出现计划中断。

WebSocket 30 秒无任何消息会重连整个异常连接。单币 120 秒没有 public event 时只刷新该币
的订阅和快照，并记录 symbol-scoped `CONNECTION_LOST_GAP`。L2 `pu/u` 不连续时单独记录
`L2_SEQUENCE_GAP` 并重新取 snapshot。

## 正式起点

空数据根启动后，collector 必须完成：

1. 全部 60 个币的 public 和 market WebSocket 订阅；
2. 全部 L2 初始 snapshot；
3. 全部 60 个币的第一次 open-interest 请求；
4. discovery 和 clock 首次请求。

随后写入 raw `universe_decision` 和 `FORMAL_COLLECTION_STARTED` 事件，强制 finalize writer，
再持久化 `control/formal-start.json`。generation 1 决策在写入前必须绑定上述双重状态响应和
ticker 响应的 SHA-256。该事件时间之后的数据属于正式实验。24 小时资源观察是生产监控，
不会清空或重启已经采集的数据。

## rsync 可靠性

107 每分钟执行一个短生命周期任务：

1. 用固定私钥和 known_hosts 将 Vultr `ready/` rsync 到 `runtime/rsync/ready`；
2. 读取 manifest，将数据写入 `.partial`，fsync，校验 size 与 SHA-256；
3. 原子 rename 到 `data/raw/collector=<id>/...`，再持久化本地 manifest；
4. 生成 ACK 并 rsync 到 Vultr `control/acks/`；
5. Vultr 只有在 ACK 的 chunk ID 和 SHA-256 都匹配后才删除 ready 数据。

禁止使用 `--remove-source-files`。暂存镜像不是永久数据，下一次同步可删除已从 Vultr GC 的
镜像文件；`data/raw` 才是 107 上的永久原始数据。

## 1C1G 性能合同

目标机器为 1 vCPU、1GiB RAM、25GB 磁盘，不允许通过减少币数或降低采集频率达标。

- Docker：`0.90 CPU`、`768MiB`、`256 PIDs`；
- 2 个稳定 hash public shards，WebSocket queue 为 4，单消息上限 2MiB；
- raw queue 总字节上限 64MiB，70% 告警，50% 恢复；
- writer batch 上限 2000 events 或 2MiB；
- RSS p95 不超过 600MiB，峰值不超过 700MiB，无 OOM；
- CPU 平均不超过 65%，p95 不超过 80%；
- event-loop lag p99 小于 100ms；
- queue p99 小于 50%，不得连续 10 秒超过 70%；
- Parquet finalize p99 小于 5 秒；
- 24 小时内无性能原因 gap，ACK 通常小于 3 分钟；
- 磁盘可用空间至少 5GiB。

首次 24 小时只做观察和判定。任何硬指标失败都应扩容或优化实现，不得改变 60 币正式合同。
