# Universe 决策

该目录保存可审计的 instrument universe 决策，不保存运行时状态。每个 bundle 包含：

- `decision.json`：来源 hash、规则、候选排名、排除原因、core/boundary/probe 分组；
- `sources/*.json.gz`：生成决策时使用的原始 Binance discovery 响应；
- `stage-{20,40,50,60}.members.txt`：同一最终名单的嵌套 canary 阶段；
- `steady-55.members.txt`：final canary 通过后使用的 50 core + 5 boundary 稳态名单。

首次 bundle 是 [`bootstrap-2026-08-10T120017Z`](bootstrap-2026-08-10T120017Z)，其
`stage-20.members.txt` 已写入 Vultr 的 `edge.yaml.example`。该 bootstrap 只有一个 24 小时
ticker 快照，只适合建立首次名单和容量 canary；进入 DAILY 更新前必须积累 discovery 历史，
再使用稳定的多日流动性统计。

stage 60 中的 5 个 probe 用于按真实结构完成 72 小时容量测试。测试完成后，它们已经满足
48 小时停留要求；下一次 DAILY control 应切换到 `steady-55.members.txt`，释放 5 个日内
new-listing probe 空位。

可直接使用 bundle 内压缩的原始来源重放 selector：

```bash
uv run ft-data-select bootstrap \
  --exchange-info universe/bootstrap-2026-08-10T120017Z/sources/exchange-info.json.gz \
  --market-tickers universe/bootstrap-2026-08-10T120017Z/sources/market-tickers.json.gz \
  --output-dir /tmp/replayed-bootstrap
```

除 `generated_at` 外，重放得到的来源 hash、排名、分组、各阶段成员和 universe hash 应一致。
不要手工编辑生成的 decision 或 members 文件；政策变化应生成一个新 bundle。
