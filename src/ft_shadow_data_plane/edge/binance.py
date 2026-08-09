from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

import aiohttp
import orjson
from websockets.asyncio.client import connect

from ft_shadow_data_plane.contracts.models import RawEventV1, StreamType
from ft_shadow_data_plane.edge.ingest import IngestCoordinator

logger = logging.getLogger(__name__)


def classify_websocket(raw: bytes) -> tuple[StreamType, str | None]:
    try:
        message = orjson.loads(raw)
    except orjson.JSONDecodeError:
        return StreamType.UNKNOWN, None
    if not isinstance(message, dict):
        return StreamType.UNKNOWN, None
    if "result" in message or ("id" in message and "data" not in message):
        return StreamType.WS_CONTROL, None
    stream = str(message.get("stream", "")).lower()
    data = message.get("data", message)
    if not isinstance(data, dict):
        return StreamType.UNKNOWN, None
    event_type = str(data.get("e", ""))
    symbol_value = data.get("s")
    if event_type == "forceOrder" and isinstance(data.get("o"), dict):
        symbol_value = data["o"].get("s")
    symbol = str(symbol_value).upper() if symbol_value else None

    if event_type == "depthUpdate":
        return (
            StreamType.RPI_DEPTH if "@rpidepth@" in stream else StreamType.DEPTH,
            symbol,
        )
    mapping = {
        "bookTicker": StreamType.BOOK_TICKER,
        "aggTrade": StreamType.AGG_TRADE,
        "trade": StreamType.TRADE,
        "markPriceUpdate": StreamType.MARK_PRICE,
        "forceOrder": StreamType.FORCE_ORDER,
        "contractInfo": StreamType.CONTRACT_INFO,
    }
    return mapping.get(event_type, StreamType.UNKNOWN), symbol


@dataclass(slots=True)
class SourceIdentity:
    collector_id: str
    boot_id: str
    segment_id: str
    connection_id: str
    _sequence: int = 0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def event(
        self,
        *,
        stream_type: StreamType,
        exchange_symbol: str | None,
        payload: bytes,
        realtime_ns: int,
        monotonic_ns: int,
        request_id: str | None = None,
        request_realtime_ns: int | None = None,
    ) -> RawEventV1:
        async with self._lock:
            self._sequence += 1
            sequence = self._sequence
        return RawEventV1(
            schema_version=1,
            exchange_symbol=exchange_symbol,
            stream_type=stream_type,
            collector_id=self.collector_id,
            boot_id=self.boot_id,
            segment_id=self.segment_id,
            connection_id=self.connection_id,
            receive_seq=sequence,
            app_receive_realtime_ns=realtime_ns,
            app_receive_monotonic_ns=monotonic_ns,
            payload_bytes=payload,
            request_id=request_id,
            request_realtime_ns=request_realtime_ns,
        )


class BinanceRestClient:
    def __init__(
        self,
        base_url: str,
        session: aiohttp.ClientSession,
        *,
        snapshot_interval_seconds: float,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._session = session
        self._snapshot_interval_seconds = snapshot_interval_seconds
        self._snapshot_lock = asyncio.Lock()
        self._last_snapshot_at = 0.0

    async def fetch(
        self, path: str, *, params: dict[str, str | int] | None = None
    ) -> tuple[bytes, int, int, str]:
        request_id = uuid4().hex
        requested_at = time.time_ns()
        async with self._session.get(
            f"{self._base_url}{path}", params=params, timeout=aiohttp.ClientTimeout(total=20)
        ) as response:
            payload = await response.read()
            observed_at = time.time_ns()
            response.raise_for_status()
        return payload, requested_at, observed_at, request_id

    async def fetch_snapshot(
        self, path: str, *, symbol: str
    ) -> tuple[bytes, int, int, str]:
        async with self._snapshot_lock:
            delay = self._snapshot_interval_seconds - (
                time.monotonic() - self._last_snapshot_at
            )
            if delay > 0:
                await asyncio.sleep(delay)
            result = await self.fetch(path, params={"symbol": symbol, "limit": 1000})
            self._last_snapshot_at = time.monotonic()
            return result


class BinanceWebSocketConnection:
    def __init__(
        self,
        *,
        url: str,
        subscriptions: tuple[str, ...],
        identity: SourceIdentity,
        ingest: IngestCoordinator,
        snapshot_requests: tuple[tuple[str, StreamType], ...],
        rest: BinanceRestClient,
        ready: asyncio.Event,
        stop: asyncio.Event,
        on_depth_gap: Callable[[str, str, StreamType, int, int], Awaitable[str]],
        on_depth_reanchored: Callable[[str, str, StreamType], Awaitable[None]],
    ) -> None:
        self._url = url
        self._subscriptions = subscriptions
        self._identity = identity
        self._ingest = ingest
        self._snapshot_requests = snapshot_requests
        self._rest = rest
        self._ready = ready
        self._stop = stop
        self._on_depth_gap = on_depth_gap
        self._on_depth_reanchored = on_depth_reanchored
        self._previous_u: dict[tuple[StreamType, str], int] = {}
        self._resync_tasks: dict[tuple[StreamType, str], asyncio.Task[None]] = {}

    async def run(self) -> None:
        subscription_id = int.from_bytes(uuid4().bytes[:4], "big")
        snapshot_task: asyncio.Task[None] | None = None
        try:
            async with connect(
                self._url,
                ping_interval=None,
                max_queue=16,
                max_size=16 * 1024 * 1024,
                close_timeout=10,
            ) as websocket:
                await websocket.send(
                    orjson.dumps(
                        {
                            "method": "SUBSCRIBE",
                            "params": list(self._subscriptions),
                            "id": subscription_id,
                        }
                    ).decode()
                )
                while not self._stop.is_set():
                    raw = await websocket.recv(decode=False)
                    realtime_ns = time.time_ns()
                    monotonic_ns = time.monotonic_ns()
                    if not isinstance(raw, bytes):
                        raw = raw.encode()
                    stream_type, symbol = classify_websocket(raw)
                    event = await self._identity.event(
                        stream_type=stream_type,
                        exchange_symbol=symbol,
                        payload=raw,
                        realtime_ns=realtime_ns,
                        monotonic_ns=monotonic_ns,
                    )
                    await self._ingest.put(event)
                    if stream_type is StreamType.WS_CONTROL and _is_subscription_ack(
                        raw, subscription_id
                    ):
                        if self._snapshot_requests:
                            snapshot_task = asyncio.create_task(
                                self._fetch_snapshots(),
                                name=f"snapshots-{self._identity.connection_id}",
                            )
                        else:
                            self._ready.set()
                    if snapshot_task is not None and snapshot_task.done():
                        await snapshot_task
                        snapshot_task = None
                        self._ready.set()
                    if stream_type in {StreamType.DEPTH, StreamType.RPI_DEPTH} and symbol:
                        await self._check_depth_sequence(stream_type, symbol, raw)
                    for key, task in tuple(self._resync_tasks.items()):
                        if task.done():
                            await task
                            del self._resync_tasks[key]
        finally:
            tasks = list(self._resync_tasks.values())
            if snapshot_task is not None:
                tasks.append(snapshot_task)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_snapshots(self) -> None:
        for symbol, stream_type in self._snapshot_requests:
            await self._fetch_snapshot(symbol, stream_type)

    async def _fetch_snapshot(self, symbol: str, stream_type: StreamType) -> None:
        delay = 1.0
        for attempt in range(5):
            try:
                is_rpi = stream_type is StreamType.RPI_DEPTH_SNAPSHOT
                path = "/fapi/v1/rpiDepth" if is_rpi else "/fapi/v1/depth"
                payload, requested_at, observed_at, request_id = (
                    await self._rest.fetch_snapshot(path, symbol=symbol)
                )
                event = await self._identity.event(
                    stream_type=stream_type,
                    exchange_symbol=symbol,
                    payload=payload,
                    realtime_ns=observed_at,
                    monotonic_ns=time.monotonic_ns(),
                    request_id=request_id,
                    request_realtime_ns=requested_at,
                )
                await self._ingest.put(event)
                return
            except (aiohttp.ClientError, TimeoutError):
                if attempt == 4:
                    raise
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10)

    async def _check_depth_sequence(
        self, stream_type: StreamType, symbol: str, raw: bytes
    ) -> None:
        message = _json_object(raw)
        data = message.get("data", message)
        if not isinstance(data, dict):
            raise ValueError("depth data must be an object")
        previous = int(data["pu"])
        final = int(data["u"])
        key = (stream_type, symbol)
        expected = self._previous_u.get(key)
        self._previous_u[key] = final
        if expected is None or previous == expected or key in self._resync_tasks:
            return
        gap_id = await self._on_depth_gap(
            self._identity.connection_id, symbol, stream_type, expected, previous
        )
        snapshot_type = (
            StreamType.RPI_DEPTH_SNAPSHOT
            if stream_type is StreamType.RPI_DEPTH
            else StreamType.DEPTH_SNAPSHOT
        )

        async def reanchor() -> None:
            await self._fetch_snapshot(symbol, snapshot_type)
            await self._on_depth_reanchored(gap_id, symbol, stream_type)

        self._resync_tasks[key] = asyncio.create_task(
            reanchor(), name=f"depth-reanchor-{self._identity.connection_id}-{symbol}"
        )


def public_subscriptions(instruments: tuple[str, ...], *, d0_enabled: bool) -> tuple[str, ...]:
    streams = [
        stream
        for symbol in instruments
        for stream in (f"{symbol.lower()}@bookTicker", f"{symbol.lower()}@depth@100ms")
    ]
    if d0_enabled:
        streams.extend(f"{symbol.lower()}@trade" for symbol in instruments)
        streams.extend(f"{symbol.lower()}@rpiDepth@500ms" for symbol in instruments)
    return tuple(streams)


def market_subscriptions(instruments: tuple[str, ...]) -> tuple[str, ...]:
    streams = [
        stream
        for symbol in instruments
        for stream in (
            f"{symbol.lower()}@aggTrade",
            f"{symbol.lower()}@markPrice@1s",
            f"{symbol.lower()}@forceOrder",
        )
    ]
    streams.append("!contractInfo")
    return tuple(streams)


def shard_instruments(instruments: tuple[str, ...], count: int) -> tuple[tuple[str, ...], ...]:
    shard_count = min(count, len(instruments))
    shards: list[list[str]] = [[] for _ in range(shard_count)]
    for index, symbol in enumerate(sorted(instruments)):
        shards[index % shard_count].append(symbol)
    return tuple(tuple(shard) for shard in shards)


def _is_subscription_ack(raw: bytes, expected_id: int) -> bool:
    try:
        value: Any = orjson.loads(raw)
    except orjson.JSONDecodeError:
        return False
    return (
        isinstance(value, dict)
        and value.get("id") == expected_id
        and value.get("result") is None
    )


def _json_object(raw: bytes) -> dict[str, Any]:
    value = orjson.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("Binance payload must be an object")
    return value
