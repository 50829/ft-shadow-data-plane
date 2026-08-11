from __future__ import annotations

import asyncio
import gzip
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import orjson
import pytest

from ft_shadow_data_plane.contracts.models import GapReason, RawEventV1, StreamType
from ft_shadow_data_plane.edge.binance import SourceIdentity, public_subscriptions
from ft_shadow_data_plane.edge.config import load_edge_config
from ft_shadow_data_plane.edge.sources import (
    ConnectionHandle,
    RestPollers,
    RouteRunner,
    _advance_deadline,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class FakeRest:
    def __init__(self) -> None:
        self.calls: Counter[str] = Counter()

    async def fetch(
        self, path: str, *, params: dict[str, str | int] | None = None
    ) -> tuple[bytes, int, int, str]:
        assert path == "/fapi/v1/openInterest"
        assert params is not None
        symbol = str(params["symbol"])
        self.calls[symbol] += 1
        if symbol == "ETHUSDT" and self.calls[symbol] == 1:
            raise aiohttp.ClientConnectionError("temporary failure")
        return b'{"symbol":"BTCUSDT","openInterest":"1","time":1}', 1, 2, symbol


class FakeIngest:
    def __init__(self, stop: asyncio.Event) -> None:
        self.events: list[RawEventV1] = []
        self._stop = stop

    async def put(self, event: RawEventV1) -> None:
        self.events.append(event)
        if {item.exchange_symbol for item in self.events} == {"BTCUSDT", "ETHUSDT"}:
            self._stop.set()


class DailyKlineRest:
    def __init__(self) -> None:
        self.params: list[dict[str, str | int]] = []

    async def fetch(
        self, path: str, *, params: dict[str, str | int] | None = None
    ) -> tuple[bytes, int, int, str]:
        assert path == "/fapi/v1/klines"
        assert params is not None
        self.params.append(params)
        day_ms = 86_400_000
        rows = [
            [
                open_ms,
                "1",
                "1",
                "1",
                "1",
                "1",
                open_ms + day_ms - 1,
                "10000000",
                100000,
            ]
            for open_ms in range(int(params["startTime"]), int(params["endTime"]) + 1, day_ms)
        ][: int(params["limit"])]
        return orjson.dumps(rows), 1, 2, f"request-{len(self.params)}"


class RecordingIngest:
    def __init__(self) -> None:
        self.events: list[RawEventV1] = []

    async def put(self, event: RawEventV1) -> None:
        self.events.append(event)


class FakeQueues:
    async def wait_until_resumable(self) -> None:
        return


class FakeGaps:
    def __init__(self) -> None:
        self.opened: list[tuple[GapReason, tuple[str, ...], tuple[StreamType, ...]]] = []
        self.closed: list[tuple[str, GapReason, tuple[str, ...], tuple[StreamType, ...]]] = []

    async def open(
        self,
        reason: GapReason,
        *,
        exchange_symbols: tuple[str, ...],
        stream_types: tuple[StreamType, ...],
        affected_from_realtime_ns: int | None = None,
        detail: str,
    ) -> str:
        self.opened.append((reason, exchange_symbols, stream_types))
        return "gap-ethusdt"

    async def close(
        self,
        gap_id: str,
        reason: GapReason,
        *,
        exchange_symbols: tuple[str, ...],
        stream_types: tuple[StreamType, ...],
        detail: str,
    ) -> None:
        self.closed.append((gap_id, reason, exchange_symbols, stream_types))


class RouteGaps:
    def __init__(self) -> None:
        self.opened: list[tuple[GapReason, str | None]] = []
        self.closed: list[tuple[str, GapReason, str | None]] = []

    async def open(
        self,
        reason: GapReason,
        *,
        connection_id: str | None = None,
        exchange_symbols: tuple[str, ...],
        stream_types: tuple[StreamType, ...],
        affected_from_realtime_ns: int | None = None,
        detail: str,
    ) -> str:
        self.opened.append((reason, connection_id))
        return "gap-connection"

    async def close(
        self,
        gap_id: str,
        reason: GapReason,
        *,
        connection_id: str | None = None,
        exchange_symbols: tuple[str, ...],
        stream_types: tuple[StreamType, ...],
        detail: str,
    ) -> None:
        self.closed.append((gap_id, reason, connection_id))


@pytest.mark.asyncio
async def test_open_interest_failure_is_tracked_per_symbol() -> None:
    stop = asyncio.Event()
    ingest = FakeIngest(stop)
    gaps = FakeGaps()
    ready: list[str] = []

    async def ignore_discovery(value: object) -> None:
        return None

    pollers = RestPollers(
        config=SimpleNamespace(open_interest_interval_seconds=0.01),  # type: ignore[arg-type]
        instruments=("BTCUSDT", "ETHUSDT"),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=ingest,  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=gaps,  # type: ignore[arg-type]
        rest=FakeRest(),  # type: ignore[arg-type]
        stop=stop,
        on_ready=ready.append,
        on_discovery=ignore_discovery,
    )

    await asyncio.wait_for(pollers._open_interest_loop(), timeout=1)

    assert gaps.opened == [(GapReason.CONNECTION_LOST, ("ETHUSDT",), (StreamType.OPEN_INTEREST,))]
    assert gaps.closed == [
        (
            "gap-ethusdt",
            GapReason.CONNECTION_LOST,
            ("ETHUSDT",),
            (StreamType.OPEN_INTEREST,),
        )
    ]
    assert ready == ["open_interest"]


def test_fixed_rate_deadline_skips_missed_slots_without_drifting() -> None:
    assert _advance_deadline(100.0, 30.0, 101.0) == 130.0
    assert _advance_deadline(100.0, 30.0, 170.0) == 190.0


@pytest.mark.asyncio
async def test_daily_kline_evidence_appends_only_the_new_complete_day(
    tmp_path: Path,
) -> None:
    config_path = PROJECT_ROOT / "deploy/vultr/edge.yaml.example"
    config = load_edge_config(config_path).model_copy(update={"data_root": tmp_path})
    rest = DailyKlineRest()
    ingest = RecordingIngest()
    stop = asyncio.Event()

    async def ignore_discovery(value: object) -> None:
        return None

    pollers = RestPollers(
        config=config,
        instruments=("BTCUSDT",),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=ingest,  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=SimpleNamespace(),  # type: ignore[arg-type]
        rest=rest,  # type: ignore[arg-type]
        stop=stop,
        on_ready=lambda _: None,
        on_discovery=ignore_discovery,
    )
    exchange_info = orjson.dumps(
        {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "onboardDate": 1,
                }
            ]
        }
    )
    first = await pollers._fetch_daily_klines(
        exchange_info, datetime(2026, 8, 17, 23, 50, tzinfo=UTC)
    )
    cache = tmp_path / "control/universe/observations/2026-08-17/daily-klines.json.gz"
    cache.parent.mkdir(parents=True)
    cache.write_bytes(gzip.compress(first))

    second = await pollers._fetch_daily_klines(
        exchange_info, datetime(2026, 8, 18, 23, 50, tzinfo=UTC)
    )

    assert [value["limit"] for value in rest.params] == [14, 1]
    payload = orjson.loads(second)
    assert len(payload["symbols"]["BTCUSDT"]["payload"]) == 14
    assert [event.stream_type for event in ingest.events] == [
        StreamType.DAILY_KLINES,
        StreamType.DAILY_KLINES,
    ]


@pytest.mark.asyncio
async def test_live_update_only_changes_replaced_symbol_subscriptions() -> None:
    stop = asyncio.Event()
    initial = tuple(f"S{index:03}USDT" for index in range(60))
    proposed = tuple(sorted((*initial[:-1], "NEWUSDT")))
    runner = RouteRunner(
        name="public-0",
        url="wss://example.invalid/stream",
        subscriptions=public_subscriptions(initial, d0_enabled=False),
        instruments=initial,
        stream_types=(StreamType.BOOK_TICKER, StreamType.DEPTH),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=SimpleNamespace(),  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=stop,
        subscriptions_for=lambda values: public_subscriptions(values, d0_enabled=False),
    )

    updating = asyncio.create_task(runner.update_instruments(proposed))
    update = await asyncio.wait_for(runner._updates.get(), timeout=0.5)
    assert set(update.remove) == {
        "s059usdt@bookTicker",
        "s059usdt@depth@100ms",
    }
    assert set(update.add) == {
        "newusdt@bookTicker",
        "newusdt@depth@100ms",
    }
    assert update.snapshot_requests == (("NEWUSDT", StreamType.DEPTH_SNAPSHOT),)
    update.completion.set_result(None)
    await updating
    assert runner.instruments == proposed


@pytest.mark.asyncio
async def test_liveness_detects_depth_silence_while_book_ticker_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    stop = asyncio.Event()
    gaps = FakeGaps()
    monkeypatch.setattr("ft_shadow_data_plane.edge.sources.time.monotonic", lambda: clock[0])
    runner = RouteRunner(
        name="public-0",
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@bookTicker", "btcusdt@depth@100ms"),
        instruments=("BTCUSDT",),
        stream_types=(StreamType.BOOK_TICKER, StreamType.DEPTH),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=gaps,  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=stop,
        subscriptions_for=lambda values: public_subscriptions(values, d0_enabled=False),
        liveness_timeout_seconds=120,
    )

    waits = 0

    async def advance_clock(_seconds: float) -> None:
        nonlocal waits
        waits += 1
        if waits == 1:
            clock[0] = 121.0
            runner._mark_event(StreamType.BOOK_TICKER, "BTCUSDT")
        else:
            stop.set()

    async def refresh(symbols: tuple[str, ...]) -> None:
        assert symbols == ("BTCUSDT",)
        stop.set()

    monkeypatch.setattr(runner, "_wait_or_stop", advance_clock)
    monkeypatch.setattr(runner, "_refresh_symbols", refresh)

    await asyncio.wait_for(runner.liveness_loop(), timeout=0.5)

    assert gaps.opened == [(GapReason.CONNECTION_LOST, ("BTCUSDT",), (StreamType.DEPTH,))]


@pytest.mark.asyncio
async def test_liveness_gap_stays_open_until_the_stream_proves_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    gaps = FakeGaps()
    runner = RouteRunner(
        name="market-0",
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@markPrice@1s",),
        instruments=("BTCUSDT",),
        stream_types=(StreamType.MARK_PRICE,),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=gaps,  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=stop,
        subscriptions_for=lambda values: tuple(
            f"{symbol.lower()}@markPrice@1s" for symbol in values
        ),
        liveness_timeout_seconds=0.01,
        liveness_stream_types=(StreamType.MARK_PRICE,),
    )

    async def make_stale(_seconds: float) -> None:
        runner._last_event[(StreamType.MARK_PRICE, "BTCUSDT")] = (
            runner._last_event[(StreamType.MARK_PRICE, "BTCUSDT")][0] - 1,
            1,
        )

    async def refresh_without_event(_symbols: tuple[str, ...]) -> None:
        return None

    monkeypatch.setattr(runner, "_wait_or_stop", make_stale)
    monkeypatch.setattr(runner, "_refresh_symbols", refresh_without_event)

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(runner.liveness_loop(), timeout=0.5)

    assert gaps.opened == [(GapReason.CONNECTION_LOST, ("BTCUSDT",), (StreamType.MARK_PRICE,))]
    assert gaps.closed == []


@pytest.mark.asyncio
async def test_route_timeout_opens_gap_and_recovery_closes_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    gaps = RouteGaps()
    runner = RouteRunner(
        name="market-0",
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@aggTrade",),
        instruments=("BTCUSDT",),
        stream_types=(StreamType.AGG_TRADE,),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=gaps,  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=stop,
    )
    starts = 0

    async def fail_silently() -> None:
        raise TimeoutError("no websocket message for 30s")

    async def wait_until_cancelled() -> None:
        await asyncio.Event().wait()

    async def start_connection() -> ConnectionHandle:
        nonlocal starts
        starts += 1
        identity = SourceIdentity("tokyo01", "boot", f"segment-{starts}", f"connection-{starts}")
        if starts == 1:
            task = asyncio.create_task(fail_silently())
        else:
            task = asyncio.create_task(wait_until_cancelled())
            asyncio.get_running_loop().call_soon(stop.set)
        return ConnectionHandle(identity, asyncio.Event(), asyncio.Event(), task)

    async def skip_retry_delay(seconds: float) -> None:
        return None

    monkeypatch.setattr(runner, "_start_ready_connection", start_connection)
    monkeypatch.setattr(runner, "_wait_or_stop", skip_retry_delay)

    await asyncio.wait_for(runner.run(), timeout=0.5)

    assert gaps.opened == [(GapReason.CONNECTION_LOST, "connection-1")]
    assert gaps.closed == [("gap-connection", GapReason.CONNECTION_LOST, "connection-2")]
