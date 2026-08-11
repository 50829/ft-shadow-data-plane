from __future__ import annotations

from datetime import UTC, datetime, timedelta

import orjson

from ft_shadow_data_plane.central.selector import DiscoverySnapshot

DAY_MS = 86_400_000


def symbols(start: int, stop: int) -> tuple[str, ...]:
    return tuple(f"S{index:03}USDT" for index in range(start, stop))


def formal_roles() -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    return symbols(0, 50), symbols(50, 55), symbols(65, 70)


def liquidity_snapshot(
    observed_at: datetime,
    *,
    inactive: str | None = None,
    incomplete: frozenset[str] = frozenset(),
    low_trades: frozenset[str] = frozenset(),
) -> DiscoverySnapshot:
    cutoff = datetime.combine(observed_at.date(), datetime.min.time(), UTC)
    cutoff_ms = int(cutoff.timestamp() * 1000)
    start_ms = cutoff_ms - 14 * DAY_MS
    exchange_rows = []
    kline_rows: dict[str, dict[str, object]] = {}
    depth_rows: dict[str, list[dict[str, object]]] = {}
    book_rows = []
    for index, symbol in enumerate(symbols(0, 70)):
        age_days = 100 if index < 65 else 20 - (index - 65)
        exchange_rows.append(
            {
                "symbol": symbol,
                "contractType": "PERPETUAL",
                "status": "SETTLING" if symbol == inactive else "TRADING",
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "onboardDate": int((cutoff - timedelta(days=age_days)).timestamp() * 1000),
            }
        )
        volume = 100_000_000 - index * 1_000_000
        trades = 20_000 if symbol in low_trades else 200_000 - index * 1_000
        bars = [
            [
                open_ms,
                "1",
                "1",
                "1",
                "1",
                "1",
                open_ms + DAY_MS - 1,
                str(volume),
                trades,
            ]
            for open_ms in range(start_ms, cutoff_ms, DAY_MS)
        ]
        if symbol in incomplete:
            bars.pop()
        kline_rows[symbol] = {"payload": bars, "response_sha256": "a" * 64}
        depth_rows[symbol] = [
            {
                "payload": {
                    "lastUpdateId": sample,
                    "bids": [["99.99", "1000"], ["99.5", "1000"]],
                    "asks": [["100.01", "1000"], ["100.5", "1000"]],
                },
                "response_sha256": "b" * 64,
                "round": sample,
            }
            for sample in range(1, 4)
        ]
        book_rows.append(
            {"symbol": symbol, "bidPrice": "99.99", "askPrice": "100.01"}
        )
    exchange_info = orjson.dumps({"symbols": exchange_rows})
    daily_klines = orjson.dumps(
        {
            "schema_version": 1,
            "window_start_ms": start_ms,
            "window_end_exclusive_ms": cutoff_ms,
            "symbols": kline_rows,
        }
    )
    liquidity_depth = orjson.dumps(
        {
            "schema_version": 1,
            "book_tickers": [
                {"sample": sample, "payload": book_rows, "response_sha256": "c" * 64}
                for sample in range(1, 6)
            ],
            "symbols": depth_rows,
        }
    )
    return DiscoverySnapshot(
        observed_at=observed_at,
        exchange_info=exchange_info,
        exchange_info_confirmation=exchange_info,
        market_tickers=b"[]",
        daily_klines=daily_klines,
        liquidity_depth=liquidity_depth,
    )
