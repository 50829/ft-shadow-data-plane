from __future__ import annotations

import gzip
from datetime import UTC, datetime, timedelta
from pathlib import Path

import orjson

from ft_shadow_data_plane.central.selector import (
    select_bootstrap_universe,
    write_bootstrap_bundle,
)


def test_bootstrap_selection_is_auditable_and_nested(tmp_path: Path) -> None:
    as_of = datetime(2026, 8, 10, 12, tzinfo=UTC)
    exchange_symbols = []
    tickers = []
    for index in range(90):
        symbol = f"S{index:03}USDT"
        exchange_symbols.append(
            {
                "symbol": symbol,
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "onboardDate": int((as_of - timedelta(days=index + 1)).timestamp() * 1000),
            }
        )
        tickers.append({"symbol": symbol, "quoteVolume": str(10_000 - index)})
    exchange_symbols.extend(
        (
            {
                "symbol": "STOPPEDUSDT",
                "contractType": "PERPETUAL",
                "status": "SETTLING",
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "onboardDate": 1,
            },
            {
                "symbol": "USDCUSDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "quoteAsset": "USDC",
                "marginAsset": "USDC",
                "onboardDate": 1,
            },
        )
    )
    exchange_info = orjson.dumps(
        {"serverTime": int(as_of.timestamp() * 1000), "symbols": exchange_symbols}
    )
    market_tickers = orjson.dumps(tickers)

    selection = select_bootstrap_universe(
        exchange_info,
        market_tickers,
        generated_at=as_of,
    )

    assert tuple(selection.stages) == (20, 40, 50, 60)
    assert all(
        set(selection.stages[smaller]) < set(selection.stages[larger])
        for smaller, larger in zip((20, 40, 50), (40, 50, 60), strict=True)
    )
    assert selection.decision["buckets"]["probe"] == [
        "S000USDT",
        "S001USDT",
        "S002USDT",
        "S003USDT",
        "S004USDT",
    ]
    assert selection.decision["counts"]["exclusion_reasons"] == {
        "not_target_quote_or_margin_asset": 1,
        "not_trading": 1,
    }
    assert len(selection.steady_members) == 55
    assert set(selection.steady_members) < set(selection.stages[60])

    write_bootstrap_bundle(
        selection,
        tmp_path,
        exchange_info_bytes=exchange_info,
        market_tickers_bytes=market_tickers,
    )
    assert gzip.decompress((tmp_path / "sources/exchange-info.json.gz").read_bytes()) == (
        exchange_info
    )
    assert gzip.decompress((tmp_path / "sources/market-tickers.json.gz").read_bytes()) == (
        market_tickers
    )
    assert (tmp_path / "stage-20.members.txt").read_text(encoding="ascii").splitlines() == list(
        selection.stages[20]
    )
    assert (tmp_path / "steady-55.members.txt").read_text(encoding="ascii").splitlines() == list(
        selection.steady_members
    )
