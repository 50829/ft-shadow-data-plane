from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from uuid import uuid4

import aiohttp
from websockets.exceptions import ConnectionClosed

from ft_shadow_data_plane.contracts.models import GapReason, StreamType
from ft_shadow_data_plane.edge.binance import (
    BinanceRestClient,
    BinanceWebSocketConnection,
    SourceIdentity,
    market_subscriptions,
    public_subscriptions,
    shard_instruments,
)
from ft_shadow_data_plane.edge.config import EdgeConfig
from ft_shadow_data_plane.edge.gaps import GapJournal
from ft_shadow_data_plane.edge.ingest import IngestCoordinator
from ft_shadow_data_plane.edge.queue import ByteBoundedQueues, QueueOverloaded

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ConnectionHandle:
    identity: SourceIdentity
    ready: asyncio.Event
    stop: asyncio.Event
    task: asyncio.Task[None]


class RouteRunner:
    def __init__(
        self,
        *,
        name: str,
        url: str,
        subscriptions: tuple[str, ...],
        instruments: tuple[str, ...],
        stream_types: tuple[StreamType, ...],
        collector_id: str,
        boot_id: str,
        ingest: IngestCoordinator,
        queues: ByteBoundedQueues,
        gaps: GapJournal,
        rest: BinanceRestClient,
        rotation_seconds: int,
        overlap_seconds: int,
        service_stop: asyncio.Event,
        rotation_offset_seconds: float = 0,
        d0_enabled: bool = False,
        on_ready: Callable[[str], None] | None = None,
    ) -> None:
        self._name = name
        self._url = url
        self._subscriptions = subscriptions
        self._instruments = instruments
        self._stream_types = stream_types
        self._collector_id = collector_id
        self._boot_id = boot_id
        self._ingest = ingest
        self._queues = queues
        self._gaps = gaps
        self._rest = rest
        self._rotation_seconds = rotation_seconds
        self._next_rotation_seconds = rotation_seconds + rotation_offset_seconds
        self._overlap_seconds = overlap_seconds
        self._service_stop = service_stop
        self._d0_enabled = d0_enabled
        self._on_ready = on_ready or (lambda _: None)

    async def run(self) -> None:
        current: ConnectionHandle | None = None
        gap_id: str | None = None
        gap_reason = GapReason.CONNECTION_LOST
        while not self._service_stop.is_set():
            if current is None:
                try:
                    current = await self._start_ready_connection()
                    self._on_ready(self._name)
                    if gap_id is not None:
                        await self._gaps.close(
                            gap_id,
                            gap_reason,
                            connection_id=current.identity.connection_id,
                            exchange_symbols=self._instruments,
                            stream_types=self._stream_types,
                            detail=f"{self._name} recovered",
                        )
                        gap_id = None
                except QueueOverloaded as exc:
                    gap_reason = GapReason.INGEST_OVERLOAD
                    if gap_id is None:
                        gap_id = await self._open_gap(gap_reason, str(exc))
                    await self._queues.wait_until_resumable()
                    continue
                except asyncio.CancelledError:
                    return
                except (aiohttp.ClientError, ConnectionClosed, OSError, TimeoutError) as exc:
                    if gap_id is None:
                        gap_id = await self._open_gap(GapReason.CONNECTION_LOST, str(exc))
                    await self._wait_or_stop(5)
                    continue

            outcome = await self._wait_current(current)
            if outcome == "stop":
                await _stop_handle(current)
                return
            if outcome == "failed":
                error = _task_error(current.task)
                gap_reason = (
                    GapReason.INGEST_OVERLOAD
                    if isinstance(error, QueueOverloaded)
                    else GapReason.CONNECTION_LOST
                )
                gap_id = await self._open_gap(
                    gap_reason,
                    str(error or "connection closed"),
                    connection_id=current.identity.connection_id,
                )
                current = None
                if gap_reason is GapReason.INGEST_OVERLOAD:
                    await self._queues.wait_until_resumable()
                else:
                    await self._wait_or_stop(1)
                continue

            try:
                replacement = await self._start_ready_connection()
            except asyncio.CancelledError:
                await _stop_handle(current)
                return
            except (
                QueueOverloaded,
                aiohttp.ClientError,
                ConnectionClosed,
                OSError,
                TimeoutError,
            ) as exc:
                logger.warning("replacement connection failed route=%s error=%s", self._name, exc)
                await self._wait_or_stop(30)
                continue
            await self._wait_or_stop(self._overlap_seconds)
            if self._service_stop.is_set():
                await _stop_handle(replacement)
                await _stop_handle(current)
                return
            await _stop_handle(current)
            current = replacement

    async def _start_ready_connection(self) -> ConnectionHandle:
        connection_id = f"{self._name}-{uuid4().hex}"
        identity = SourceIdentity(
            collector_id=self._collector_id,
            boot_id=self._boot_id,
            segment_id=uuid4().hex,
            connection_id=connection_id,
        )
        ready = asyncio.Event()
        stop = asyncio.Event()
        connection = BinanceWebSocketConnection(
            url=self._url,
            subscriptions=self._subscriptions,
            identity=identity,
            ingest=self._ingest,
            snapshot_requests=self._snapshot_requests(),
            rest=self._rest,
            ready=ready,
            stop=stop,
            on_depth_gap=self._open_depth_gap,
            on_depth_reanchored=self._close_depth_gap,
        )
        task = asyncio.create_task(connection.run(), name=connection_id)
        handle = ConnectionHandle(identity, ready, stop, task)
        ready_task = asyncio.create_task(ready.wait())
        service_stop_task = asyncio.create_task(self._service_stop.wait())
        done, pending = await asyncio.wait(
            (ready_task, task, service_stop_task),
            timeout=600,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for pending_task in pending:
            if pending_task is not task:
                pending_task.cancel()
        if service_stop_task in done and self._service_stop.is_set():
            await _stop_handle(handle)
            raise asyncio.CancelledError
        if task in done:
            error = _task_error(task)
            if error is not None:
                raise error
            raise OSError("websocket closed before becoming ready")
        if ready_task not in done:
            await _stop_handle(handle)
            raise TimeoutError("websocket did not become snapshot-ready within 600 seconds")
        return handle

    def _snapshot_requests(self) -> tuple[tuple[str, StreamType], ...]:
        if StreamType.DEPTH not in self._stream_types:
            return ()
        requests = [(symbol, StreamType.DEPTH_SNAPSHOT) for symbol in self._instruments]
        if self._d0_enabled:
            requests.extend(
                (symbol, StreamType.RPI_DEPTH_SNAPSHOT) for symbol in self._instruments
            )
        return tuple(requests)

    async def _open_depth_gap(
        self,
        connection_id: str,
        symbol: str,
        stream_type: StreamType,
        expected: int,
        received: int,
    ) -> str:
        return await self._gaps.open(
            GapReason.L2_SEQUENCE,
            connection_id=connection_id,
            exchange_symbols=(symbol,),
            stream_types=(stream_type,),
            detail=f"expected_pu={expected} received_pu={received}",
        )

    async def _close_depth_gap(
        self, gap_id: str, symbol: str, stream_type: StreamType
    ) -> None:
        await self._gaps.close(
            gap_id,
            GapReason.L2_SEQUENCE,
            exchange_symbols=(symbol,),
            stream_types=(stream_type,),
            detail="fresh snapshot received; central must still validate the bridge",
        )

    async def _wait_current(self, handle: ConnectionHandle) -> str:
        rotation = asyncio.create_task(asyncio.sleep(self._next_rotation_seconds))
        stopping = asyncio.create_task(self._service_stop.wait())
        done, pending = await asyncio.wait(
            (handle.task, rotation, stopping), return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            if task is not handle.task:
                task.cancel()
        if stopping in done and self._service_stop.is_set():
            return "stop"
        if handle.task in done:
            return "failed"
        self._next_rotation_seconds = self._rotation_seconds
        return "rotate"

    async def _open_gap(
        self, reason: GapReason, detail: str, *, connection_id: str | None = None
    ) -> str:
        return await self._gaps.open(
            reason,
            connection_id=connection_id,
            exchange_symbols=self._instruments,
            stream_types=self._stream_types,
            detail=f"{self._name}: {detail}"[:500],
        )

    async def _wait_or_stop(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._service_stop.wait(), timeout=seconds)
        except TimeoutError:
            pass


class RestPollers:
    def __init__(
        self,
        *,
        config: EdgeConfig,
        instruments: tuple[str, ...],
        collector_id: str,
        boot_id: str,
        ingest: IngestCoordinator,
        queues: ByteBoundedQueues,
        gaps: GapJournal,
        rest: BinanceRestClient,
        stop: asyncio.Event,
        on_ready: Callable[[str], None],
    ) -> None:
        self._config = config
        self._instruments = instruments
        self._ingest = ingest
        self._queues = queues
        self._gaps = gaps
        self._rest = rest
        self._stop = stop
        self._on_ready = on_ready
        self._oi_identity = SourceIdentity(
            collector_id, boot_id, uuid4().hex, f"rest-open-interest-{uuid4().hex}"
        )
        self._discovery_identity = SourceIdentity(
            collector_id, boot_id, uuid4().hex, f"rest-discovery-{uuid4().hex}"
        )

    async def run(self) -> None:
        await asyncio.gather(
            self._open_interest_loop(), self._discovery_loop(), self._clock_loop()
        )

    async def _open_interest_loop(self) -> None:
        ready_symbols: set[str] = set()
        interval = self._config.open_interest_interval_seconds
        instrument_count = len(self._instruments)
        await asyncio.gather(
            *(
                self._open_interest_symbol_loop(
                    symbol,
                    ready_symbols,
                    initial_delay=index * interval / instrument_count,
                )
                for index, symbol in enumerate(self._instruments)
            )
        )

    async def _open_interest_symbol_loop(
        self, symbol: str, ready_symbols: set[str], *, initial_delay: float
    ) -> None:
        gap: tuple[str, GapReason] | None = None
        streams = (StreamType.OPEN_INTEREST,)
        symbols = (symbol,)
        interval = self._config.open_interest_interval_seconds
        next_poll = time.monotonic() + initial_delay
        while not self._stop.is_set():
            await _wait_event(self._stop, max(0.0, next_poll - time.monotonic()))
            if self._stop.is_set():
                return
            try:
                await self._fetch_oi(symbol)
                gap = await self._close_poll_gap(gap, symbols, streams)
                ready_symbols.add(symbol)
                if len(ready_symbols) == len(self._instruments):
                    self._on_ready("open_interest")
            except QueueOverloaded as exc:
                gap = await self._open_poll_gap(
                    gap,
                    GapReason.INGEST_OVERLOAD,
                    symbols,
                    streams,
                    str(exc),
                )
                await self._queues.wait_until_resumable()
            except (aiohttp.ClientError, TimeoutError) as exc:
                gap = await self._open_poll_gap(
                    gap,
                    GapReason.CONNECTION_LOST,
                    symbols,
                    streams,
                    str(exc),
                )
                logger.warning("open-interest poll failed symbol=%s error=%s", symbol, exc)
            finally:
                next_poll = _advance_deadline(next_poll, interval, time.monotonic())

    async def _fetch_oi(self, symbol: str) -> None:
        payload, requested_at, observed_at, request_id = await self._rest.fetch(
            "/fapi/v1/openInterest", params={"symbol": symbol}
        )
        event = self._oi_identity.event(
            stream_type=StreamType.OPEN_INTEREST,
            exchange_symbol=symbol,
            payload=payload,
            realtime_ns=observed_at,
            monotonic_ns=time.monotonic_ns(),
            request_id=request_id,
            request_realtime_ns=requested_at,
        )
        await self._ingest.put(event)

    async def _discovery_loop(self) -> None:
        gap: tuple[str, GapReason] | None = None
        streams = (StreamType.EXCHANGE_INFO, StreamType.MARKET_TICKERS)
        while not self._stop.is_set():
            try:
                for path, stream_type in (
                    ("/fapi/v1/exchangeInfo", StreamType.EXCHANGE_INFO),
                    ("/fapi/v1/ticker/24hr", StreamType.MARKET_TICKERS),
                ):
                    payload, requested_at, observed_at, request_id = await self._rest.fetch(path)
                    event = self._discovery_identity.event(
                        stream_type=stream_type,
                        exchange_symbol=None,
                        payload=payload,
                        realtime_ns=observed_at,
                        monotonic_ns=time.monotonic_ns(),
                        request_id=request_id,
                        request_realtime_ns=requested_at,
                    )
                    await self._ingest.put(event)
                gap = await self._close_poll_gap(gap, (), streams)
                self._on_ready("discovery")
                await _wait_event(self._stop, self._config.exchange_info_interval_seconds)
            except (aiohttp.ClientError, QueueOverloaded, TimeoutError) as exc:
                reason = (
                    GapReason.INGEST_OVERLOAD
                    if isinstance(exc, QueueOverloaded)
                    else GapReason.CONNECTION_LOST
                )
                gap = await self._open_poll_gap(gap, reason, (), streams, str(exc))
                if isinstance(exc, QueueOverloaded):
                    await self._queues.wait_until_resumable()
                logger.warning("discovery poll failed error=%s", exc)
                await _wait_event(self._stop, 30)

    async def _clock_loop(self) -> None:
        gap: tuple[str, GapReason] | None = None
        streams = (StreamType.CLOCK_SAMPLE,)
        while not self._stop.is_set():
            try:
                payload, requested_at, observed_at, request_id = await self._rest.fetch(
                    "/fapi/v1/time"
                )
                event = self._discovery_identity.event(
                    stream_type=StreamType.CLOCK_SAMPLE,
                    exchange_symbol=None,
                    payload=payload,
                    realtime_ns=observed_at,
                    monotonic_ns=time.monotonic_ns(),
                    request_id=request_id,
                    request_realtime_ns=requested_at,
                )
                await self._ingest.put(event)
                gap = await self._close_poll_gap(gap, (), streams)
                self._on_ready("clock")
                await _wait_event(self._stop, self._config.clock_sample_interval_seconds)
            except (aiohttp.ClientError, QueueOverloaded, TimeoutError) as exc:
                reason = (
                    GapReason.INGEST_OVERLOAD
                    if isinstance(exc, QueueOverloaded)
                    else GapReason.CONNECTION_LOST
                )
                gap = await self._open_poll_gap(gap, reason, (), streams, str(exc))
                if isinstance(exc, QueueOverloaded):
                    await self._queues.wait_until_resumable()
                logger.warning("clock sample failed error=%s", exc)
                await _wait_event(self._stop, 10)

    async def _open_poll_gap(
        self,
        current: tuple[str, GapReason] | None,
        reason: GapReason,
        symbols: tuple[str, ...],
        streams: tuple[StreamType, ...],
        detail: str,
    ) -> tuple[str, GapReason]:
        if current is not None:
            return current
        gap_id = await self._gaps.open(
            reason,
            exchange_symbols=symbols,
            stream_types=streams,
            detail=detail[:500],
        )
        return gap_id, reason

    async def _close_poll_gap(
        self,
        current: tuple[str, GapReason] | None,
        symbols: tuple[str, ...],
        streams: tuple[StreamType, ...],
    ) -> tuple[str, GapReason] | None:
        if current is None:
            return None
        gap_id, reason = current
        await self._gaps.close(
            gap_id,
            reason,
            exchange_symbols=symbols,
            stream_types=streams,
            detail="REST polling recovered",
        )
        return None


class SourceManager:
    def __init__(
        self,
        config: EdgeConfig,
        *,
        collector_id: str,
        boot_id: str,
        ingest: IngestCoordinator,
        queues: ByteBoundedQueues,
        gaps: GapJournal,
    ) -> None:
        self._config = config
        self._collector_id = collector_id
        self._boot_id = boot_id
        self._ingest = ingest
        self._queues = queues
        self._gaps = gaps
        self._stop: asyncio.Event | None = None
        self._task: asyncio.Task[None] | None = None
        self._ready: asyncio.Event | None = None

    @property
    def running(self) -> bool:
        return self._task is not None

    def raise_if_failed(self) -> None:
        if self._task is None or not self._task.done():
            return
        error = _task_error(self._task)
        if error is not None:
            raise RuntimeError("Binance source group failed") from error
        raise RuntimeError("Binance source group stopped unexpectedly")

    async def start(self, instruments: tuple[str, ...]) -> None:
        if self._task is not None:
            raise RuntimeError("sources already running")
        self._stop = asyncio.Event()
        self._ready = asyncio.Event()
        self._task = asyncio.create_task(
            self._run(instruments, self._stop, self._ready), name="binance-sources"
        )

    async def wait_ready(self) -> None:
        if self._task is None or self._ready is None:
            raise RuntimeError("sources are not running")
        ready_task = asyncio.create_task(self._ready.wait())
        done, pending = await asyncio.wait(
            (ready_task, self._task), timeout=600, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            if task is not self._task:
                task.cancel()
        if self._task in done:
            error = _task_error(self._task)
            if error is not None:
                raise RuntimeError("Binance sources failed before readiness") from error
            raise RuntimeError("Binance sources stopped before readiness")
        if ready_task not in done:
            raise TimeoutError("Binance sources did not become ready within 600 seconds")

    async def stop(self) -> None:
        if self._task is None or self._stop is None:
            return
        self._stop.set()
        await asyncio.gather(self._task, return_exceptions=True)
        self._task = None
        self._stop = None
        self._ready = None

    async def _run(
        self, instruments: tuple[str, ...], stop: asyncio.Event, ready: asyncio.Event
    ) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            rest = BinanceRestClient(
                self._config.rest_url,
                session,
                snapshot_interval_seconds=self._config.snapshot_request_interval_seconds,
            )
            routes = []
            shards = shard_instruments(instruments, self._config.public_connection_shards)
            expected = {
                *(f"public-{index}" for index in range(len(shards))),
                "market-0",
                "open_interest",
                "discovery",
                "clock",
            }
            ready_names: set[str] = set()

            def mark_ready(name: str) -> None:
                ready_names.add(name)
                if expected <= ready_names:
                    ready.set()

            for index, shard in enumerate(shards):
                public_stream_types = [StreamType.BOOK_TICKER, StreamType.DEPTH]
                if self._config.d0_enabled:
                    public_stream_types.extend((StreamType.TRADE, StreamType.RPI_DEPTH))
                snapshot_count = len(shard) * (2 if self._config.d0_enabled else 1)
                rotation_offset = index * (
                    snapshot_count * self._config.snapshot_request_interval_seconds
                    + self._config.connection_overlap_seconds
                )
                routes.append(
                    RouteRunner(
                        name=f"public-{index}",
                        url=self._config.public_ws_url,
                        subscriptions=public_subscriptions(
                            shard, d0_enabled=self._config.d0_enabled
                        ),
                        instruments=shard,
                        stream_types=tuple(public_stream_types),
                        collector_id=self._collector_id,
                        boot_id=self._boot_id,
                        ingest=self._ingest,
                        queues=self._queues,
                        gaps=self._gaps,
                        rest=rest,
                        rotation_seconds=self._config.connection_rotation_seconds,
                        rotation_offset_seconds=rotation_offset,
                        overlap_seconds=self._config.connection_overlap_seconds,
                        service_stop=stop,
                        d0_enabled=self._config.d0_enabled,
                        on_ready=mark_ready,
                    ).run()
                )
            routes.append(
                RouteRunner(
                    name="market-0",
                    url=self._config.market_ws_url,
                    subscriptions=market_subscriptions(instruments),
                    instruments=instruments,
                    stream_types=(
                        StreamType.AGG_TRADE,
                        StreamType.MARK_PRICE,
                        StreamType.FORCE_ORDER,
                        StreamType.CONTRACT_INFO,
                    ),
                    collector_id=self._collector_id,
                    boot_id=self._boot_id,
                    ingest=self._ingest,
                    queues=self._queues,
                    gaps=self._gaps,
                    rest=rest,
                    rotation_seconds=self._config.connection_rotation_seconds,
                    overlap_seconds=self._config.connection_overlap_seconds,
                    service_stop=stop,
                    on_ready=mark_ready,
                ).run()
            )
            pollers = RestPollers(
                config=self._config,
                instruments=instruments,
                collector_id=self._collector_id,
                boot_id=self._boot_id,
                ingest=self._ingest,
                queues=self._queues,
                gaps=self._gaps,
                rest=rest,
                stop=stop,
                on_ready=mark_ready,
            )
            routes.append(pollers.run())
            await asyncio.gather(*routes)


async def _stop_handle(handle: ConnectionHandle) -> None:
    handle.stop.set()
    handle.task.cancel()
    await asyncio.gather(handle.task, return_exceptions=True)


def _task_error(task: asyncio.Task[None]) -> BaseException | None:
    if task.cancelled():
        return asyncio.CancelledError()
    return task.exception()


async def _wait_event(event: asyncio.Event, delay_seconds: float) -> None:
    try:
        await asyncio.wait_for(event.wait(), timeout=delay_seconds)
    except TimeoutError:
        pass


def _advance_deadline(previous: float, interval: float, now: float) -> float:
    deadline = previous + interval
    if deadline <= now:
        missed = int((now - deadline) // interval) + 1
        deadline += missed * interval
    return deadline
