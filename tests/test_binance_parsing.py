from __future__ import annotations

import orjson
import pytest

from ft_shadow_data_plane.central.binance import parse_typed_row
from ft_shadow_data_plane.contracts.models import StreamType


@pytest.mark.parametrize(
    ("stream", "payload", "expected_field", "expected_value"),
    [
        (
            StreamType.DEPTH,
            {"e": "depthUpdate", "E": 1, "T": 2, "s": "BTCUSDT", "U": 10,
             "u": 11, "pu": 9, "b": [["100", "1"]], "a": [["101", "2"]]},
            "final_update_id",
            11,
        ),
        (
            StreamType.DEPTH_SNAPSHOT,
            {"lastUpdateId": 11, "bids": [["100", "1"]], "asks": [["101", "2"]]},
            "last_update_id",
            11,
        ),
        (
            StreamType.BOOK_TICKER,
            {"e": "bookTicker", "E": 1, "T": 2, "s": "BTCUSDT", "u": 12,
             "b": "100", "B": "1", "a": "101", "A": "2"},
            "bid_price",
            "100",
        ),
        (
            StreamType.AGG_TRADE,
            {"e": "aggTrade", "E": 1, "T": 2, "s": "BTCUSDT", "a": 3,
             "p": "100", "q": "1", "f": 4, "l": 5, "m": True},
            "aggregate_trade_id",
            3,
        ),
        (
            StreamType.TRADE,
            {"e": "trade", "E": 1, "T": 2, "s": "BTCUSDT", "t": 7,
             "p": "100", "q": "1", "m": False},
            "trade_id",
            7,
        ),
        (
            StreamType.MARK_PRICE,
            {"e": "markPriceUpdate", "E": 1, "s": "BTCUSDT", "p": "100",
             "i": "99", "P": "0", "r": "-0.0001", "T": 8},
            "funding_rate",
            "-0.0001",
        ),
        (
            StreamType.FORCE_ORDER,
            {"e": "forceOrder", "E": 1, "o": {"s": "BTCUSDT", "S": "SELL",
             "o": "LIMIT", "f": "IOC", "q": "2", "p": "100", "ap": "99",
             "X": "FILLED", "l": "1", "z": "2", "T": 2}},
            "side",
            "SELL",
        ),
        (
            StreamType.CONTRACT_INFO,
            {"e": "contractInfo", "E": 1, "s": "BTCUSDT", "ct": "PERPETUAL",
             "dt": 0, "ot": 1, "cs": "TRADING"},
            "contract_status",
            "TRADING",
        ),
        (
            StreamType.OPEN_INTEREST,
            {"symbol": "BTCUSDT", "openInterest": "123.45", "time": 2},
            "open_interest",
            "123.45",
        ),
        (
            StreamType.CLOCK_SAMPLE,
            {"serverTime": 1_786_320_000_000},
            "exchange_event_time_ms",
            1_786_320_000_000,
        ),
    ],
)
def test_formal_stream_fixtures(
    stream: StreamType,
    payload: dict[str, object],
    expected_field: str,
    expected_value: object,
) -> None:
    raw_row = {
        "stream_type": stream.value,
        "exchange_symbol": "BTCUSDT",
        "connection_id": "connection-1",
        "receive_seq": 1,
        "app_receive_realtime_ns": 10,
        "app_receive_monotonic_ns": 20,
        "payload_bytes": orjson.dumps(payload),
    }
    parsed = parse_typed_row(raw_row)
    assert parsed is not None
    assert parsed[expected_field] == expected_value


@pytest.mark.parametrize(
    ("stream", "payload"),
    [
        (StreamType.MARKET_TICKERS, [{"symbol": "BTCUSDT", "quoteVolume": "1"}]),
        (StreamType.EXCHANGE_INFO, {"symbols": [{"symbol": "BTCUSDT"}]}),
    ],
)
def test_market_wide_payload_is_valid_discovery_evidence(
    stream: StreamType, payload: object
) -> None:
    assert parse_typed_row(
        {
            "stream_type": stream.value,
            "payload_bytes": orjson.dumps(payload),
        }
    ) is None
