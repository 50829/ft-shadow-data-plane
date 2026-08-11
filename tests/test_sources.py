from __future__ import annotations

import asyncio
from collections import Counter
from types import SimpleNamespace

import aiohttp
import pytest

from ft_shadow_data_plane.contracts.models import GapReason, RawEventV1, StreamType
from ft_shadow_data_plane.edge.binance import SourceIdentity, public_subscriptions
from ft_shadow_data_plane.edge.sources import (
    ConnectionHandle,
    RestPollers,
    RouteRunner,
    _advance_deadline,
)


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

    assert gaps.opened == [
        (GapReason.CONNECTION_LOST, ("ETHUSDT",), (StreamType.OPEN_INTEREST,))
    ]
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
    assert gaps.closed == [
        ("gap-connection", GapReason.CONNECTION_LOST, "connection-2")
    ]
