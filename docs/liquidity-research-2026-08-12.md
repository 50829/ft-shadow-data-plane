# Generation 1 的 60 个合约流动性调研

> 本文保留对旧 generation 1 的诊断过程，不是 v0.3 最终名单。正式冻结结果和放宽后的
> 14 日规则见 [generation 1 流动性决策](bootstrap-liquidity-decision-2026-08-12.md)。

## 结论

截至 `2026-08-11T18:01:41Z` 的 Binance USD-M 官方滚动 24 小时行情，当前 60 个合约并非
“60 个都具有同等高流动性”，而是一个有意混合的实验样本：50 个 core 承担主要市场覆盖，
5 个 boundary 测量纳入边界，5 个 probe 观察新合约。

- 60 个合约合计 `18.032B USDT` 滚动 24h `quoteVolume`，占 524 个合格合约的
  `85.794%`；交易数占 `60.014%`。高覆盖率主要由 BTC、ETH 等头部合约贡献，不能据此推断
  每个成员都很深。
- 50 个 core 占全体合格市场成交额 `84.047%`，当前 Top10 全是 core；core 的成交额中位数
  `69.829M USDT`、点差中位数 `1.592 bps`。但 core 仍有 12 个当前名次在第 55 名之后，
  `ACTUSDT` 和 `IOTXUSDT` 的成交额已低于 `10M USDT`。
- 5 个 boundary 已明显分化：`HOME/BANANAS31/AAVE` 当前排名 35/46/53，
  `ESP/THE` 已降到 130/170。这反映滚动 24h 成交活跃度变化，也说明 boundary 本来就不是
  长期稳定组。
- probe 的离散度最大：`GRVT/CAP` 当前排名 17/34，`GRAM` 为 121，`O/DATAIP` 为
  342/389。probe 点差中位数 `4.455 bps`，±10 bps 较薄侧深度中位数仅 `1.594K USDT`；
  后两者滚动 24h 成交额仅 `1.605M/1.309M USDT`，交易数 `40,406/12,794`。
- 可见盘口也高度偏斜。60 币 ±10 bps 较薄侧名义金额中位数为 `3.373K USDT`，BTC/ETH
  分别为 `14.615M/7.893M USDT`。因此“总成交额覆盖高”不等于所有币都适合大额即时成交。
- 当前组合适合作为“稳定主体 + 边界 + 新上市探索”的研究 universe；如果实验目标其实是
  “全部 60 个都是高流动性合约”，那么 probe 设计和当前约束都不满足这个目标。

本次没有修改线上 universe，也没有用调研截面触发换币。

## 取证范围与时间

Vultr 当前状态来自：

```text
/srv/ft-data-rsync/control/universe/active.json
generation: 1
created_at/effective_at: 2026-08-11T16:38:03.976676Z
reason: formal_bootstrap
universe_hash: b23c75e3c24e39da2ed02a0ec9b39a1b0d80588bd99bd47489857cc53974d8cf
file SHA-256: 2538032bbbc97ef90c129ff8bcccf1e2576f2c4273059a253db483fb3aaa7493
file size: 1,135 bytes
```

调研于 `2026-08-11T18:01:41.425Z` 开始抓取全市场响应。60 个深度快照的 Binance
事件时间覆盖 `2026-08-11T18:02:25.316Z` 至 `18:02:48.856Z`。因此成交额/交易数是一次
滚动 24h 截面，点差/深度是约 24 秒内依次取得的瞬时截面，不是同时成交结果，也不是长期
统计量。

本次按运行代码的口径，从 `exchangeInfo` 中筛选两次状态检查所要求的：

```text
status=TRADING
contractType=PERPETUAL
quoteAsset=USDT
marginAsset=USDT
```

当前单次 `exchangeInfo` 中先有 527 个合约通过 Binance 状态/合约/资产字段过滤；其中中文
symbol `币安人生USDT`、`我踏马来了USDT`、`龙虾USDT` 不符合生产代码的
`^[A-Z0-9]{1,30}$` 规则；配合 `quoteAsset=marginAsset=USDT` 后，项目实际 eligibility
pool 为 524，524 个均有 24h ticker。
本文排名分母均为 524。
生产代码的双重状态确认见
[`selector.py`](../src/ft_shadow_data_plane/central/selector.py#L78-L96)，字段过滤见
[`selector.py`](../src/ft_shadow_data_plane/central/selector.py#L323-L332)，symbol 正则定义见
[`models.py`](../src/ft_shadow_data_plane/contracts/models.py#L12)。Binance 字段定义见官方
[Exchange Information](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information)
与 [24hr Ticker Price Change Statistics](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/24hr-Ticker-Price-Change-Statistics)。

## 指标口径

| 指标 | 定义 | 能说明什么 | 不能说明什么 |
|---|---|---|---|
| 24h 成交额 | 官方 ticker 的 `quoteVolume`，单位 USDT | 滚动 24h 换手活跃度代理 | 不是 UTC 自然日，也不直接等于可成交深度 |
| 交易数 | 官方 ticker 的 `count` | 滚动窗口内成交频率代理 | 不区分交易大小，不能排除大量小额交易 |
| 排名 | 在上述 524 个合格合约中按 `quoteVolume` 降序 | 当前相对活跃度 | 是单一时点滚动窗口，不是长期排名 |
| 点差 | 深度快照最优档 `(ask-bid)/mid*10,000` | 瞬时 top-of-book 摩擦 | 不代表全天点差或实际滑点 |
| ±10/50 bps 较薄侧深度 | 分别对阈值内买/卖档位求 `price*quantity`，取两侧较小值 | 保守比较两个交易方向的瞬时可见名义金额 | 不保证可全部成交，忽略延迟、撤单、冲击和手续费 |

每个深度请求使用官方
[Order Book](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book)
接口的 `limit=1000`。分组汇总采用买卖两侧较小值；逐合约原始表另外保留双边合计作为
盘口规模诊断，不能把合计理解为一笔单向订单的可成交量。逐响应检查末档后，所有
±10 bps 统计均完整落在返回范围内；BTC 的第 1000 档买卖报价仍在 ±50 bps 内，所以 BTC
的 ±50 bps 数值 `87.449M USDT` 只是下界，其余合约的 ±50 bps 带宽均未被 1000 档上限截断。

## 分组汇总

| 角色 | 数量 | 24h 成交额合计 | 市场成交额占比 | 交易数占比 | 成交额中位数 | 排名范围（中位） | 点差中位 / P90 | 较薄侧 ±10 bps 中位 | 较薄侧 ±50 bps 中位 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 全部 | 60 | 18.032B | 85.794% | 60.014% | 62.706M | 1–389 (36.5) | 1.628 / 5.394 bps | 3.373K | 37.804K |
| core | 50 | 17.665B | 84.047% | 54.282% | 69.829M | 1–131 (31.5) | 1.592 / 5.394 bps | 6.042K | 42.631K |
| boundary | 5 | 165.034M | 0.785% | 2.561% | 40.138M | 35–170 (53) | 1.184 / 1.547 bps | 2.452K | 24.956K |
| probe | 5 | 201.990M | 0.961% | 3.172% | 9.412M | 17–389 (121) | 4.455 / 6.513 bps | 1.594K | 18.343K |

524 个合格合约的总 `quoteVolume` 为 `21.017B USDT`、总交易数为 `128,047,046`。
当前 Top10 全部被选中且均为 core；Top20 选中 19 个，Top50 选中 40 个，Top55 选中
43 个，Top100 选中 52 个。Top50 中的 40 个由 36 core、2 boundary、2 probe 构成。

## 为什么初始名单与当前排名不同

generation 1 不是 collector 在 `2026-08-11` 启动时重新从 ticker 自动选出的。初始化代码直接
复制部署 YAML 的预置 50/5/5 名单，见
[`edge/universe.py`](../src/ft_shadow_data_plane/edge/universe.py#L52-L80)；启动时的两次
`exchangeInfo` 和一次 ticker 只确认资格、绑定 source hash 并生成 evaluation，见
[`edge/universe.py`](../src/ft_shadow_data_plane/edge/universe.py#L82-L114)。

预置名单的原始依据可从 Git 历史复核：

```bash
git show 4a2214b^:universe/bootstrap-2026-08-10T120017Z/decision.json
```

该决策 `as_of=2026-08-10T12:00:17.613Z`，只使用一个滚动 24h `quoteVolume` 截面：先保留
最新 5 个合约作 probe，再从非 probe 的老合约中取 50 个 core，随后取排名 53–57 的
boundary。决策文件自身也明确写明“只有一个 24h snapshot，不能声称是 7 日流动性统计”。
由于两个高成交额 probe 占全市场第 15/26 名，core 在全市场排名中延伸到第 52 名。

| 角色 | 合约 | bootstrap 排名 / 成交额 | 当前排名 / 成交额 |
|---|---|---:|---:|
| boundary | ESPUSDT | 53 / 35.718M | 130 / 8.468M |
| boundary | THEUSDT | 54 / 34.093M | 170 / 5.254M |
| boundary | BANANAS31USDT | 55 / 33.889M | 46 / 46.639M |
| boundary | HOMEUSDT | 56 / 33.319M | 35 / 64.535M |
| boundary | AAVEUSDT | 57 / 33.116M | 53 / 40.138M |
| probe | GRVTUSDT | 15 / 177.551M | 17 / 124.712M |
| probe | CAPUSDT | 26 / 75.867M | 34 / 64.951M |
| probe | GRAMUSDT | 178 / 5.161M | 121 / 9.412M |
| probe | OUSDT | 319 / 1.812M | 342 / 1.605M |
| probe | DATAIPUSDT | 464 / 0.693M | 389 / 1.309M |

历史 decision 的 SHA-256 为
`e7934be36031ab74eb0a7e44d9946814b33c3c1fc93c82ac4e16c0389535e023`。其中旧的
20/40/50/60 stage 字段只是已删除 canary 方案留下的历史取证，不在当前 v0.2 运行树或启动
流程中；当前 collector 是一次性启动 60 个合约。

## 逐合约截面

单位：成交额为 `M USDT`；深度为 `K USDT`。点差与深度均来自每个合约自己的同一份
`limit=1000` 深度快照。下表的深度列是买卖双边合计，仅用于完整展示盘口规模；分组比较和
风险判断使用的是两侧较小值。

| 角色 | 合约 | 排名 | 24h 成交额 | 交易数 | 点差 bps | ±10 bps 双边合计 | ±50 bps 双边合计 |
|---|---|---:|---:|---:|---:|---:|---:|
| boundary | HOMEUSDT | 35 | 64.535M | 1,318,723 | 1.184 | 11.301K | 140.487K |
| boundary | BANANAS31USDT | 46 | 46.639M | 1,277,155 | 0.995 | 4.519K | 39.972K |
| boundary | AAVEUSDT | 53 | 40.138M | 280,096 | 1.148 | 137.361K | 962.445K |
| boundary | ESPUSDT | 130 | 8.468M | 222,448 | 1.398 | 5.098K | 56.060K |
| boundary | THEUSDT | 170 | 5.254M | 180,914 | 1.646 | 0.973K | 19.251K |
| core | BTCUSDT | 1 | 6,317.683M | 1,849,693 | 0.016 | 39,614.071K | 87,449.169K |
| core | ETHUSDT | 2 | 4,604.416M | 2,550,559 | 0.054 | 16,051.911K | 109,419.867K |
| core | SOLUSDT | 3 | 1,044.357M | 989,748 | 1.330 | 5,043.926K | 23,855.893K |
| core | XRPUSDT | 4 | 658.125M | 1,209,374 | 0.983 | 1,393.280K | 8,370.028K |
| core | BEATUSDT | 5 | 429.751M | 5,524,954 | 10.747 | 3.184K | 53.029K |
| core | ZECUSDT | 6 | 409.369M | 1,534,913 | 0.212 | 551.986K | 8,103.868K |
| core | TUTUSDT | 7 | 392.342M | 8,482,929 | 1.004 | 8.766K | 85.377K |
| core | BLUAIUSDT | 8 | 391.361M | 10,174,630 | 3.903 | 1.545K | 20.906K |
| core | CYSUSDT | 9 | 362.205M | 6,377,138 | 1.366 | 12.513K | 76.766K |
| core | HYPEUSDT | 10 | 299.777M | 960,611 | 0.185 | 635.298K | 4,616.707K |
| core | DOGEUSDT | 12 | 238.955M | 607,326 | 1.408 | 1,156.218K | 5,199.554K |
| core | BNBUSDT | 13 | 232.675M | 708,845 | 0.164 | 2,551.057K | 7,387.247K |
| core | LINKUSDT | 14 | 155.811M | 586,037 | 1.163 | 303.048K | 1,853.495K |
| core | PUMPUSDT | 15 | 145.755M | 697,409 | 3.717 | 58.757K | 529.639K |
| core | ADAUSDT | 16 | 125.461M | 362,826 | 5.389 | 310.604K | 1,842.092K |
| core | PAXGUSDT | 18 | 120.574M | 294,545 | 0.023 | 2,163.542K | 9,856.891K |
| core | WLDUSDT | 19 | 109.720M | 514,673 | 2.982 | 124.707K | 842.416K |
| core | BICOUSDT | 20 | 108.756M | 2,156,220 | 2.567 | 8.207K | 37.857K |
| core | NEARUSDT | 22 | 99.834M | 252,636 | 6.408 | 240.138K | 1,464.173K |
| core | 1000PEPEUSDT | 23 | 98.165M | 714,430 | 0.358 | 271.575K | 1,470.913K |
| core | SUIUSDT | 25 | 86.182M | 360,716 | 1.472 | 439.336K | 2,029.333K |
| core | BMTUSDT | 27 | 81.661M | 1,518,300 | 4.410 | 1.918K | 33.606K |
| core | BLESSUSDT | 29 | 76.411M | 2,080,311 | 0.757 | 6.315K | 56.744K |
| core | UNIUSDT | 30 | 75.771M | 354,203 | 2.700 | 131.349K | 707.366K |
| core | AVAXUSDT | 31 | 71.605M | 445,245 | 1.609 | 126.689K | 1,021.532K |
| core | KAITOUSDT | 32 | 68.052M | 1,114,892 | 1.568 | 31.743K | 323.196K |
| core | GWEIUSDT | 36 | 64.101M | 938,324 | 4.131 | 2.503K | 27.929K |
| core | TAOUSDT | 37 | 61.312M | 547,549 | 0.502 | 120.851K | 1,284.127K |
| core | HEIUSDT | 38 | 57.883M | 1,693,069 | 1.340 | 6.085K | 34.537K |
| core | TSTUSDT | 39 | 57.173M | 865,540 | 6.209 | 3.611K | 31.814K |
| core | ENAUSDT | 41 | 56.955M | 515,873 | 1.120 | 91.660K | 619.998K |
| core | MMTUSDT | 43 | 53.692M | 623,870 | 4.763 | 8.774K | 66.374K |
| core | BANKUSDT | 44 | 48.424M | 744,030 | 2.724 | 12.878K | 84.432K |
| core | SKYAIUSDT | 45 | 48.113M | 899,142 | 1.167 | 3.011K | 27.052K |
| core | ONDOUSDT | 47 | 45.870M | 240,925 | 3.001 | 169.882K | 1,063.003K |
| core | EPICUSDT | 49 | 44.291M | 1,322,319 | 2.691 | 4.541K | 34.861K |
| core | BTWUSDT | 51 | 40.962M | 1,409,157 | 2.470 | 4.629K | 37.885K |
| core | MUBARAKUSDT | 52 | 40.707M | 831,492 | 5.439 | 2.010K | 34.129K |
| core | COOKIEUSDT | 56 | 35.886M | 1,267,090 | 1.426 | 2.585K | 25.271K |
| core | ONUSDT | 60 | 32.434M | 762,353 | 2.948 | 4.718K | 47.694K |
| core | PENGUUSDT | 62 | 31.940M | 282,915 | 1.575 | 41.173K | 436.069K |
| core | XMRUSDT | 72 | 24.602M | 333,674 | 0.258 | 43.620K | 414.140K |
| core | BOMEUSDT | 77 | 22.162M | 545,351 | 1.298 | 7.847K | 103.831K |
| core | ACEUSDT | 80 | 20.230M | 651,638 | 1.882 | 2.888K | 24.794K |
| core | USUSDT | 86 | 16.758M | 925,044 | 2.968 | 2.391K | 33.248K |
| core | NILUSDT | 90 | 15.878M | 321,857 | 5.554 | 2.853K | 47.083K |
| core | XANUSDT | 98 | 12.834M | 513,196 | 4.335 | 1.239K | 13.574K |
| core | PEOPLEUSDT | 113 | 10.738M | 263,759 | 1.172 | 12.806K | 72.110K |
| core | ACTUSDT | 128 | 8.615M | 425,457 | 2.954 | 2.675K | 25.780K |
| core | IOTXUSDT | 131 | 8.331M | 159,433 | 3.913 | 5.435K | 40.773K |
| probe | GRVTUSDT | 17 | 124.712M | 2,451,796 | 3.245 | 5.719K | 45.526K |
| probe | CAPUSDT | 34 | 64.951M | 1,493,670 | 3.772 | 4.530K | 40.553K |
| probe | GRAMUSDT | 121 | 9.412M | 62,375 | 7.556 | 10.098K | 250.630K |
| probe | OUSDT | 342 | 1.605M | 40,406 | 4.455 | 2.321K | 24.786K |
| probe | DATAIPUSDT | 389 | 1.309M | 12,794 | 4.949 | 3.637K | 38.052K |

按更保守的“买卖两侧较小值”口径，以下 11 个合约的 ±10 bps 深度不足 `1K USDT`：

| 合约 | 角色 | 较薄侧 ±10 bps | 点差 |
|---|---|---:|---:|
| BEATUSDT | core | 0.225K | 10.747 bps |
| BMTUSDT | core | 0.535K | 4.410 bps |
| XANUSDT | core | 0.576K | 4.335 bps |
| OUSDT | probe | 0.635K | 4.455 bps |
| GRAMUSDT | probe | 0.655K | 7.556 bps |
| MUBARAKUSDT | core | 0.679K | 5.439 bps |
| ACTUSDT | core | 0.687K | 2.954 bps |
| COOKIEUSDT | core | 0.754K | 1.426 bps |
| BLUAIUSDT | core | 0.757K | 3.903 bps |
| GWEIUSDT | core | 0.944K | 4.131 bps |
| TSTUSDT | core | 0.945K | 6.209 bps |

这是瞬时风险标记，不是剔除阈值。尤其 `BEATUSDT` 同时具有高滚动成交额、宽点差和很薄的
较弱一侧，说明只用 `quoteVolume` 会把“交易很活跃”和“当前可承载大额订单”混为一谈。

## 风险与运行规则评估

### 1. 自动选择只使用一个流动性代理

当前 selector 只把最近 1–7 个每日观察里的 `quoteVolume` 做算术均值后排名，见
[`selector.py`](../src/ft_shadow_data_plane/central/selector.py#L98-L116)。每日 23:50 UTC 的
请求见 [`edge/sources.py`](../src/ft_shadow_data_plane/edge/sources.py#L569-L633)。Binance 的
24h ticker 是以请求时刻为终点的滚动窗口，不是 UTC 自然日；正常每日样本约隔 24 小时，
窗口基本相邻，只有服务启动时立即抓取的额外样本可能与当日 23:50 样本明显重叠。

`quoteVolume` 不进入点差、深度、指定金额冲击、OI、波动率或异常成交过滤。本调研补充的
盘口指标目前仅用于诊断，不参与自动换币。

### 2. probe 没有流动性下限

probe 对非 core 合约按 `onboardDate` 新到旧排列，成交额只用于同上市时间排序；代码没有
最低成交额、交易数、点差或深度门槛，见
[`selector.py`](../src/ft_shadow_data_plane/central/selector.py#L161-L176)。因此
`OUSDT/DATAIPUSDT` 很薄并非 selector 失效，而是当前“发现最新市场”目标的直接结果。
`onboardDate` 是交易所提供的上架时间戳，不等于连续可交易历史或首笔成交证明。

### 3. bootstrap 单快照与 14 天 core dwell 会延迟纠偏

当前 50 core 是单个滚动 24h 截面初始化的，而正式规则要求累积 7 个观察后才在周一评估
core；新成员需进入前 45，老成员名次超过 55 才有退出资格，且 core 至少停留 14 天，
每周最多替换 5 个，见 [`selector.py`](../src/ft_shadow_data_plane/central/selector.py#L117-L159)
及 [`edge.yaml.example`](../deploy/vultr/edge.yaml.example#L68-L80)。

当前已有 12 个 core 名次超过 55，其中 3 个名次超过 100；但 generation 1 于 8 月 11 日建立，
正常情况下 14 日 dwell 和周一边界意味着首次可因流动性排名替换 core 的时间不会早于
`2026-08-31T00:00:00Z`。停止交易的强制替换不受此正常轮换节奏约束。

### 4. boundary retention 的设计和实现排名空间不一致

设计文档写的是：排除 core/probe 后，boundary 目标取候选池 Top5，现有成员在该候选池
Top10 内可保留。但实现的 `protected_boundary` 使用全市场 `rank <= 10`，见
[`selector.py`](../src/ft_shadow_data_plane/central/selector.py#L178-L202)。当前全市场 Top10
全部都是 core，所以没有任何 boundary 能进入该保护集合；设计中的 boundary Top10 滞回
实际上基本不生效，只剩 48h dwell 和每天最多替换 1 个的限速。

这是需要修正并加回归测试的实现偏差。它不会让采集立即丢数据，但会让 boundary 的轮换
语义与已记录的实验设计不一致。

## 判断

这组样本对当前实验设计是合理但不完美的：

1. core 已覆盖最重要的头部市场，但 bootstrap 只依赖单个滚动窗口，长尾 core 需要等正式
   7 样本/14 日 dwell 后逐步稳定；不应把 generation 1 当作成熟的长期 Top50。
2. boundary 正在提供预期的“排名升降”信息，不过 retention 的代码口径应先修正，避免后续
   实验成员解释发生偏差。
3. probe 的薄流动性是研究设计的一部分。分析时必须把 role 作为分层变量，不能把 60 币
   合并后声称样本具有统一流动性质量。
4. 若后续研究要求可执行性，应把持续采集的 book/depth 数据计算为时间加权点差、固定名义
   金额冲击与深度分位数；单次 REST 截面只能作为当前健康检查。

## API 响应与 SHA-256

所有 URL 均为 Binance 官方一手 REST API，无鉴权参数。本次响应原始字节未经格式化后计算
SHA-256。

| 请求 | 精确 URL / 参数 | 响应字节 | SHA-256 |
|---|---|---:|---|
| Server Time | `https://fapi.binance.com/fapi/v1/time` | 28 | `f99f8efd3421c9bb6126d7364c8ece62449f4dadfe3d3f8653d707985ad34923` |
| Exchange Information | `https://fapi.binance.com/fapi/v1/exchangeInfo` | 1,062,174 | `341c3d05fe9f9f254a8c76ef948e6fb7927ccbb481eb0f9644cfba5c717bb5d2` |
| 24hr Tickers | `https://fapi.binance.com/fapi/v1/ticker/24hr`（无 `symbol`，全市场） | 272,057 | `375a6cca59e36ec880109dde02e6a05993fab71be849c497c1956022363bcfdf` |
| Book Tickers | `https://fapi.binance.com/fapi/v1/ticker/bookTicker`（无 `symbol`，全市场） | 110,435 | `719295efee2b02cb60a625a4039fcb333be8c6764078eae71e632df29aeb54ed` |
| 60 份 Order Book | `https://fapi.binance.com/fapi/v1/depth?symbol=<SYMBOL>&limit=1000` | 合计 2,316,051 | 见下表 |

全市场请求的主机抓取区间为 `2026-08-11T18:01:41.425Z` 至 `18:01:43.042Z`。
`exchangeInfo.serverTime=2026-08-11T18:00:16.058Z`；ticker 中所选合约的 open time 覆盖
`2026-08-10T18:01:00Z` 至 `18:02:00Z`、close time 覆盖
`2026-08-11T18:01:20.674Z` 至 `18:01:41.093Z`。这些逐 symbol 时间差来自 Binance 的
滚动窗口对齐规则，不代表本地抓取持续了 24 小时。

为便于整体校验，按文件名 C locale 排序的 60 行 `sha256sum depth-*.json` 文本本身的
SHA-256 是
`92803ee227a85b45b582a8ca4c41c0ed01ba5a1274a79320ff0ccdf8f8a85434`。
以下为每份完整响应的 `symbol SHA-256 bytes`：

```text
1000PEPEUSDT eb19924aea7e0796aa4ea022ef3ab2be34213499662b46ab8cd1ccdb5a366679 45833
AAVEUSDT f9009559516008266e733653e3fd8a874da8030b9f981b5cd064e498d7a713b8 35536
ACEUSDT 12e9c03a3355a962ef7d841e9d66e2d8fdae6e99f5bdac43775cdfeb8a59cfa1 44186
ACTUSDT 38f57f3b2d09b4b9319d6919251b0dbef538ef016d850cfcb9993aa7788eb838 36474
ADAUSDT 22352068c7bef86b658cc61c70c06dbba484d65dc0edf6c35d3be6eac292cf12 37980
AVAXUSDT 8c5557f2b3bda802018acd59bdbca64b6ccf299cef615b5dae9ffbd16019057b 32742
BANANAS31USDT fbc2acd98aa163609b8d5650fdeed8274cf80b260b0ec535a8790f1fc397f929 43126
BANKUSDT 2ac3c5794a4012b1d8298f3628ba39f67843a428a0c2eccd5ea5a4509af9713e 42482
BEATUSDT 4cf09f9305e94a891bac4e771f732d3a4f9c9db8fb9cbd0c2806bce5f3e52854 39223
BICOUSDT 22228c0f3c08b519de7fe64066fd490e91c812cd6e555a0d4c90931248b05b72 41987
BLESSUSDT 0852a04db71d71e38156a24e6ffad6baf84632fd0d7f80a76471e800232d6779 42643
BLUAIUSDT 3c7b0614889852e7a28412e2925ab5a4dfede2a45e4369914a8367666e65bf3d 42784
BMTUSDT f48efae4817230b37c3a5a6047497ac05c4b1d26e83faeac00efd1a013c135a6 42480
BNBUSDT fa5fd1499c01ac28c7c389f6b2d26e36b1d05b856dae4b0ffdf0540fc5bada0b 38776
BOMEUSDT a3bb6ab8f274ca3b1933fe3e91d82dfd509e030e8ce834b95a763f45cb1e42d2 44884
BTCUSDT 3ce8f382b63a9e5ffdac2ecce1da8f60fac77092651393c811c28d3b4a190ce7 42104
BTWUSDT 185434c273492ea2d99f07cf01da58f4656c31b904c8b08bd5dce8b6e929de62 39805
CAPUSDT 22976d37b8a786d879e54b1f9556e96f68a4520ec59df8bb27dc47185abd3108 41732
COOKIEUSDT 2c4cfad4d2538a14db59ff99de655bebc44ed2f17c7396918fc22e1e80cc6a19 42443
CYSUSDT 900980f006a8b504daa4756ffc3f3b5e07fbaefd9c6e37af7105e11143801f25 38902
DATAIPUSDT cb31d7a206abb5c40bd4c99c1960c99f409d63be13dacb6e4f0163a60ff54a38 6580
DOGEUSDT c85453e06332699ccb250bc0e6a91a5b3bdb18723a39551a63a06ea23eaaaf8f 42073
ENAUSDT bd3367d49debefa72fdebec7662f5908f812e219507551ce8c36c3712539b889 42069
EPICUSDT e7a31efec086dacf7f2826cfabcdf53c5fa185c41df7368414f0e7622b0c5190 39239
ESPUSDT d72522e79a1eb0428d04f32a9b1330f0f4808d89207f178d4b406aedd58d4fd3 32253
ETHUSDT 12ca40e5e20026bfb404c7ab6565cf00e3b0baf797f4bfd1a8526430b38add59 40994
GRAMUSDT 59f8862b4d745bc998ed21e5a214292927b38a9264d58399801b1dcb294c9710 27718
GRVTUSDT 2a6640918eb2240ee07df00418ed897c9cf6779fe8c2613a19fa20317183416f 37540
GWEIUSDT d9196f43ffbe61b23728d3c237f4fa2a5fe747ee3cf1007b54f86a8b11cc0dfd 35992
HEIUSDT 1502d28963dad43f23b3d1cda90b85460ad8d03f641542d383da0dd075b88c6f 42434
HOMEUSDT 1574db290c14d3921ff09a9033e14e049a5db50537810dd671231b154be28ec2 43477
HYPEUSDT aabbdf51664add523c26c8f909997730f434409578e2c09697d15dd533e5de7a 41767
IOTXUSDT 98246727ccb4422b7fcbb7a135f22412010e8a9e014acc3a4bd3f59bdaa0a157 37844
KAITOUSDT 3d02f5b3bd7d79ca627f633088cf6b8d7047d9655257db1d301f905d91ca8410 41849
LINKUSDT 2f129f82a27e0342113277921108b17a8ec5b231f0d5a8f6b904a4c15da9080c 36869
MMTUSDT dfc637c34eb188e37da1104d130ac0f3a0d131e5f984ccfd98f47e661f084f86 37392
MUBARAKUSDT 6cd3cb33ed452d0fded1791b8f4ecb15984b61e948071d328af52405284c4937 34447
NEARUSDT fc459b0d4447c9b50b589c683ad9bd21153102988650b8e2d41a63d64bbc3fc9 31183
NILUSDT 79d30e4f9d5285394b2cc1f2cea54f38cd9e7078bd541b70314668c6024e495f 34459
ONDOUSDT 4a3375cc52611fb439f7b23a8bafdcac35b292bd88b9501ce834d6d759d4e2e4 44963
ONUSDT 02f60033d163336f55027196331f7e4d0e053f53019be18118da0922ec440f0d 39629
OUSDT 0d8bc7d25ed01bd60d99fb90ee420ccf1511ce832b358030f4b1640221f17141 13104
PAXGUSDT 88a57a3044c77886d3f66edc4bb05ff1c25c5560b56eea144462fed768c6ee1c 44119
PENGUUSDT de0869974811fab8f9ca7f02b03dbe49d46a4c2aea85f758979f853a2b438479 41094
PEOPLEUSDT 76a1fafed08880dd4e87dcbf430d2fed9164dc811b1b5de75e88a855bff450ac 42728
PUMPUSDT 7abc06685f1b62abaae66fbc9758b60e2401de3b3f714e3f04fcc58a98bdedd3 45288
SKYAIUSDT b7c22986043808ed8fac096869c4239a23b8fc2c67208171e4e4da03d292183c 41228
SOLUSDT 268d2cff9a771980d64c81588026146da662ac7b49e43dd65e3967cb6e040c4e 41423
SUIUSDT 3d56a6df9ad0b0a0bc0bec669dea0c5b1f990a1b74b82f94429a4de6118ed2a1 42906
TAOUSDT c3c736be520f62e29e692ff36dc5312d01746b1cfa7d9495f5fe3b93613c9e69 38845
THEUSDT f0a3ef9a5af2b94719a6990ee6e30de1b13a0de07a55a2401f0b0b8f8273280e 29301
TSTUSDT f0ea3ebd0f97fadc6b72386b78fe3cc3bbfd0a1f782cf79ba8823dcbe32d034d 38313
TUTUSDT ebf9cbd58d186633b7ee0f9194b21d0e59ad9a2f7c7bf805267b9050a15ea374 41652
UNIUSDT f35c4dc0ea0cd1d8da939d1562a8edd91d08545fcebf859c18a2cf2ff9a2c233 32996
USUSDT cc635a2c591a1affb0111a2a70f8d407853e9666001586ad8af3b7d2360e675a 41308
WLDUSDT 38d2c5ac37f9c9254e375648b0187512b9d898a37d0f12a0afefd5de6562e9cd 41133
XANUSDT 7f797f2be4f9e3a4f3d7e6c078686295e3d2869ed65a91f41633d97cfd70ec1f 36332
XMRUSDT 150fb0c02c40dec8a02762a6e22b65594c377a2d95327439af8435831767fba5 38358
XRPUSDT 9c491907b3d97a53bae090abe93269493265ff5326b7d90aaf307f9d689612c1 40272
ZECUSDT 3b2cb31d8b89190e92604e4e71001780ee856027f9ad85b18a52ea4d041e3e2f 38758
```

Vultr 正式启动时保存的三份原始响应也已验证解压后 SHA-256，且与 active generation 1 的
`source_hashes` 完全一致：

```text
exchange-info.json                 5db4864dd593d9a88b32427e0129457e2052d4e52a8532e3f8a54c95b3ee6d9e
exchange-info-confirmation.json    87046ff3b317ce96b0e23092aaea2fac8fb2fcab5a2104ec518cb68e25e35c80
market-tickers.json                573885a18f4544ab5e30693837c0544e81e89b9ce8e5fb8914245140b3676096
```

它们的 observation 时间是 `2026-08-11T16:38:04.131147Z`。这三份启动证据用于正式开采前
的资格确认和 generation 绑定；本文 `18:01Z` 的新截面用于回答“现在的流动性怎样”，两者
用途不同。

## 局限

- REST 深度是 24 秒内依次抓取，无法用于严格的横截面同步比较；排名 ticker 与深度又相差
  约一分钟。
- `quoteVolume` 和 `count` 是滚动窗口值，窗口边界逐 symbol 略有差异，且可能受短期波动、
  新币热度和大量小额交易影响。
- 可见委托可以在成交前撤销；本报告没有计算手续费、排队位置、实际 market impact、OI、
  波动率或资金费率。
- 单个深度快照不能估计日内分位数。更可靠的流动性结论应使用 107 已持久化的连续
  `depth/bookTicker/aggTrade` 数据完成时间加权研究。
- 初始 bootstrap 源文件已从当前工作树删除，只能通过 Git 历史复核；它证明当时的单快照
  选择过程，但不是当前 24h 数值的来源。
