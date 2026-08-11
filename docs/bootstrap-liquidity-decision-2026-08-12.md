# generation 1 流动性决策（2026-08-12）

## 最终结论

本报告冻结 v0.3 正式采集的全新 `generation 1`。证据观察时间为
`2026-08-11T19:22:50.929112Z`（北京时间 2026-08-12 03:22:50），只使用 Binance 官方
USD-M Futures 接口和最近 14 个完整 UTC 日，不读取或迁移旧 generation 数据。

最终合格 stable 池为 67 个。50 个 core 与 5 个 boundary 共占 55 个槽位，剩余 12 个为
stable 备份；完整 14 日 probe 合格池为 72 个。余量足以启动正式采集，后续低于 65 时报警，
不足 55 时 fail closed、保留现有 60 币且不产生换币决策。

## generation 1

Core（50）：

```text
1000BONKUSDT 1000PEPEUSDT 1000SHIBUSDT AAVEUSDT ADAUSDT AKEUSDT
ALLOUSDT ARBUSDT AVAXUSDT BANKUSDT BCHUSDT BNBUSDT BTCUSDT COTIUSDT
DEXEUSDT DOGEUSDT DOTUSDT ENAUSDT ESPORTSUSDT ETHUSDT EULUSDT
FARTCOINUSDT FETUSDT FILUSDT HBARUSDT HYPEUSDT INJUSDT KAITOUSDT
LINKUSDT LITUSDT LTCUSDT NEARUSDT ONDOUSDT ONUSDT PAXGUSDT PENGUUSDT
PUMPUSDT RIFUSDT SOLUSDT SUIUSDT TAOUSDT TRUMPUSDT TRXUSDT UBUSDT
UNIUSDT WLDUSDT XLMUSDT XMRUSDT XRPUSDT ZECUSDT
```

Boundary（5）：

```text
APTUSDT ETCUSDT LDOUSDT WLFIUSDT ZAMAUSDT
```

Probe（5）：

```text
CAPUSDT ESPUSDT REUSDT SLXUSDT XAUTUSDT
```

对应 `universe_hash`：

```text
b03cadcd9f6162772e957a0a2f507ee39c78c1c754c280a7816921f2b25a3909
```

## 准入规则

- core/boundary 必须有 14/14 个完整 UTC 日，合约年龄至少 30 日；bootstrap probe 也要求
  14/14 个完整日，日常新增 probe 至少要求 7/7 个完整日；
- 每日 USDT 成交额中位数 `>=10M`、P25 `>=5M`、最小值 `>=3M`、CV `<=1.2`；
- 每日交易数中位数 `>=100K`、P25 `>=50K`、最小值 `>=25K`；
- 5 次全市场 bookTicker 和 3 次 `limit=100` depth 的最大点差均 `<=10 bps`；
- 3 次 depth 的较薄一侧均满足 `+/-10 bps >=800 USDT`、
  `+/-50 bps >=10K USDT`；
- 两次包围证据抓取的 exchangeInfo 都必须是 USDT 报价、USDT 保证金、`TRADING` 的
  perpetual contract。

初始严格规则的 stable 备份只有 2 个，因此将单日最低成交额由 `5M` 定向放宽为 `3M`、
成交额 CV 上限由 `1.0` 放宽为 `1.2`。上线预演时，`ONUSDT` 的 10 bps 薄侧深度为
`981.60227 USDT`，仅比原 `1K` 门槛低 1.84%；把该门槛定向调整到 `800 USDT` 后，stable
池由 64 增至 67。`+/-50 bps` 的 `10K` 深度门槛和其他质量门槛均未放宽。

## 证据绑定

机器可读的冻结决策见 [formal-generation-1-evidence.json](formal-generation-1-evidence.json)，
其 SHA-256 同时写入 Vultr 配置：

```text
c77498c9be3ae0547c983db06d166ac9711945067aaa7ac4a7bf6623dbd65910
```

原始响应的 canonical content hashes，依次为首次 exchangeInfo、确认 exchangeInfo、24h ticker、
14 日 Kline evidence、盘口 evidence：

```text
e09ed865f74881ec28e5ea8b581e090667576a28369d59077f2299069f1be04d
0c48d453f9d8b0bd67efe0d06e0f1bfdf44d20d153710f239765eea8a77a637b
c243f18e2e6c4d78c5828ca32e4689fee7cc9aa44670395a6839f970f7c6ffd8
b5546f628b2a0c96ec6da81d5e150e753a5befc7875d0657572924f3431bf29f
a22c18983a5b9b64773c2e11bcca4979571869a25e284ed94d1aa615a9a10726
```

首次启动会重新抓取同样类型的完整证据，逐个验证冻结成员仍通过角色硬门槛，并把新的实时
source hashes 绑定到 active decision。瞬时盘口使合格池内部排序改变时不会改写 generation 1；
任何冻结成员失活或跌破硬门槛则拒绝写 `FORMAL_COLLECTION_STARTED`。

## 一手来源

- [Exchange Information](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information)
- [Kline/Candlestick Data](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data)
- [Symbol Order Book Ticker](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Symbol-Order-Book-Ticker)
- [Order Book](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book)
