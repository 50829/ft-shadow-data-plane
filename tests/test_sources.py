from __future__ import annotations

import asyncio
from collections import Counter
from types import SimpleNamespace

import aiohttp
import pytest

from ft_shadow_data_plane.contracts.models import GapReason, RawEventV1, StreamType
from ft_shadow_data_plane.edge.sources import RestPollers, _advance_deadline


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


@pytest.mark.asyncio
async def test_open_interest_failure_is_tracked_per_symbol() -> None:
    stop = asyncio.Event()
    ingest = FakeIngest(stop)
    gaps = FakeGaps()
    ready: list[str] = []
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
