from __future__ import annotations

from ft_shadow_data_plane.contracts.serde import sha256_bytes

DATA_CONTRACT_V1 = b"""{
  "exchange":"binance_usdm",
  "raw_schema":1,
  "streams":[
    "agg_trade","book_ticker","clock_sample","contract_info","depth","depth_snapshot",
    "exchange_info","force_order","mark_price","market_tickers","open_interest"
  ],
  "depth_interval_ms":100,
  "depth_snapshot_limit":1000,
  "mark_price_interval_ms":1000,
  "open_interest_interval_seconds":30
}\n"""

DATA_CONTRACT_HASH_V1 = sha256_bytes(DATA_CONTRACT_V1)
