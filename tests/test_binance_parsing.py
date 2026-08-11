from __future__ import annotations

import asyncio
from types import SimpleNamespace

import orjson
import pytest

from ft_shadow_data_plane.central.binance import parse_typed_row
from ft_shadow_data_plane.contracts.models import RawEventV1, StreamType
from ft_shadow_data_plane.edge.binance import (
    BinanceWebSocketConnection,
    SourceIdentity,
    decode_websocket,
)


class StalledWebSocket:
    def __init__(self) -> None:
        self.subscription_id: int | None = None
        self.receive_calls = 0
        self.stalled = asyncio.Event()

    async def __aenter__(self) -> StalledWebSocket:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def send(self, value: str) -> None:
        message = orjson.loads(value)
        self.subscription_id = int(message["id"])

    async def recv(self, *, decode: bool) -> bytes:
        assert decode is False
        self.receive_calls += 1
        if self.receive_calls == 1:
            return orjson.dumps({"result": None, "id": self.subscription_id})
        self.stalled.set()
        await asyncio.Future()
        raise AssertionError("unreachable")


class RecordingIngest:
    def __init__(self) -> None:
        self.events: list[RawEventV1] = []

    async def put(self, event: RawEventV1) -> None:
        self.events.append(event)


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


def test_edge_depth_decode_reuses_parsed_sequence_fields() -> None:
    decoded = decode_websocket(
        orjson.dumps(
            {
                "stream": "btcusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "s": "BTCUSDT",
                    "U": 10,
                    "u": 11,
                    "pu": 9,
                    "b": [],
                    "a": [],
                },
            }
        )
    )

    assert decoded.stream_type is StreamType.DEPTH
    assert decoded.symbol == "BTCUSDT"
    assert decoded.data is not None
    assert (decoded.data["pu"], decoded.data["u"]) == (9, 11)


def test_source_identity_assigns_sequence_without_async_scheduling() -> None:
    identity = SourceIdentity("tokyo01", "boot", "segment", "connection")

    first = identity.event(
        stream_type=StreamType.AGG_TRADE,
        exchange_symbol="BTCUSDT",
        payload=b"{}",
        realtime_ns=1,
        monotonic_ns=1,
    )
    second = identity.event(
        stream_type=StreamType.AGG_TRADE,
        exchange_symbol="BTCUSDT",
        payload=b"{}",
        realtime_ns=2,
        monotonic_ns=2,
    )

    assert (first.receive_seq, second.receive_seq) == (1, 2)


@pytest.mark.asyncio
async def test_websocket_silence_fails_the_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    websocket = StalledWebSocket()
    ingest = RecordingIngest()

    monkeypatch.setattr(
        "ft_shadow_data_plane.edge.binance.connect",
        lambda *args, **kwargs: websocket,
    )

    async def open_depth_gap(*args: object) -> str:
        return "gap-depth"

    async def close_depth_gap(*args: object) -> None:
        return None

    connection = BinanceWebSocketConnection(
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@aggTrade",),
        identity=SourceIdentity("tokyo01", "boot", "segment", "connection"),
        ingest=ingest,  # type: ignore[arg-type]
        snapshot_requests=(),
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        ready=asyncio.Event(),
        stop=asyncio.Event(),
        receive_timeout_seconds=0.01,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        max_queue=4,
        max_message_bytes=2 * 1024**2,
        updates=asyncio.Queue(),
        on_depth_gap=open_depth_gap,
        on_depth_reanchored=close_depth_gap,
    )

    task = asyncio.create_task(connection.run())
    try:
        await asyncio.wait_for(websocket.stalled.wait(), timeout=0.5)
        await asyncio.sleep(0.05)
        assert task.done(), "a silent websocket remained stuck in recv()"
        with pytest.raises(TimeoutError, match="no websocket message"):
            await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
