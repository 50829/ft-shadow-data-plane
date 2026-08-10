from __future__ import annotations

import asyncio
import logging
import os
import resource
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pyarrow as pa

from ft_shadow_data_plane.contracts.data_contract import data_contract_hash_v1
from ft_shadow_data_plane.contracts.models import ControlReason, GapEventV1, GapReason
from ft_shadow_data_plane.edge.config import EdgeConfig
from ft_shadow_data_plane.edge.day_index import DayIndex
from ft_shadow_data_plane.edge.gaps import GapJournal
from ft_shadow_data_plane.edge.ingest import IngestCoordinator
from ft_shadow_data_plane.edge.queue import ByteBoundedQueues
from ft_shadow_data_plane.edge.sources import SourceManager
from ft_shadow_data_plane.edge.spool import SpoolManager
from ft_shadow_data_plane.edge.universe import UniverseStore
from ft_shadow_data_plane.edge.writer import ChunkLimits, WriterPool

logger = logging.getLogger(__name__)


class EdgeService:
    def __init__(self, config: EdgeConfig) -> None:
        self._config = config
        self._boot_id = uuid4().hex
        self._operation_lock = asyncio.Lock()
        self._stop = asyncio.Event()

        self._spool = SpoolManager(
            config.data_root,
            max_bytes=config.spool_max_bytes,
            minimum_free_bytes=config.minimum_free_bytes,
        )
        self._universe_store = UniverseStore(
            config.data_root, config.bootstrap_instruments
        )
        active = self._universe_store.initialize()
        active = self._universe_store.apply_due(
            reasons=frozenset({ControlReason.DAILY, ControlReason.CANARY_SCALE})
        ) or active
        data_contract_hash = data_contract_hash_v1(
            d0_enabled=config.d0_enabled,
            open_interest_interval_seconds=config.open_interest_interval_seconds,
        )
        self._day_index = DayIndex(config.data_root, config.collector_id)
        self._queues = ByteBoundedQueues(
            config.queue_max_bytes,
            warn_ratio=config.queue_warn_ratio,
            resume_ratio=config.queue_resume_ratio,
        )
        self._writers = WriterPool(
            config.data_root,
            collector_id=config.collector_id,
            data_contract_hash=data_contract_hash,
            universe_hash=active.universe_hash,
            queues=self._queues,
            day_index=self._day_index,
            limits=ChunkLimits(
                max_seconds=config.chunk_max_seconds,
                max_bytes=config.chunk_max_bytes,
                max_events=config.chunk_max_events,
                batch_events=config.writer_batch_events,
                batch_bytes=config.writer_batch_bytes,
            ),
        )
        self._ingest = IngestCoordinator(self._queues, self._writers)
        self._gaps = GapJournal(
            config.data_root,
            collector_id=config.collector_id,
            data_contract_hash=data_contract_hash,
            universe_hash=active.universe_hash,
            day_index=self._day_index,
        )
        self._sources = SourceManager(
            config,
            collector_id=config.collector_id,
            boot_id=self._boot_id,
            ingest=self._ingest,
            queues=self._queues,
            gaps=self._gaps,
        )
        self._storage_gap_id: str | None = None
        self._stale_gaps: tuple[GapEventV1, ...] = ()
        self._previous_cpu = _host_cpu_sample()
        self._previous_written_bytes = 0

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        self._spool.initialize()
        self._gaps.initialize()
        self._stale_gaps = self._gaps.stale_open_events()
        for gap in self._stale_gaps:
            if gap.reason is GapReason.STORAGE_EXHAUSTED:
                self._storage_gap_id = gap.gap_id
                break
        self._quarantine_incomplete_chunks()
        self._writers.start()
        try:
            if not (await asyncio.to_thread(self._spool.status)).hard_limited:
                await self._sources.start(self._universe_store.active.members)
                await self._sources.wait_ready()
                await self._close_stale_gaps()
                await self._seal_completed_days()
        except BaseException:
            await self._sources.stop()
            await self._writers.stop()
            raise

        background = [
            asyncio.create_task(self._storage_loop(), name="storage-monitor"),
            asyncio.create_task(self._midnight_loop(), name="utc-midnight-rotation"),
            asyncio.create_task(self._control_loop(), name="universe-control"),
            asyncio.create_task(self._stats_loop(), name="collector-stats"),
            asyncio.create_task(self._writers.wait_for_failure(), name="writer-health"),
        ]
        stop_task = asyncio.create_task(self._stop.wait(), name="edge-stop")
        done, _ = await asyncio.wait(
            (*background, stop_task), return_when=asyncio.FIRST_COMPLETED
        )
        failure: BaseException | None = None
        for task in done:
            if task is stop_task or task.cancelled():
                continue
            failure = task.exception()
            if failure is not None:
                logger.exception("edge background task failed", exc_info=failure)
                break
        self._stop.set()
        for task in background:
            if not task.done():
                task.cancel()
        if self._sources.running:
            await self._gaps.open(
                GapReason.COLLECTOR_STOPPED,
                exchange_symbols=self._universe_store.active.members,
                detail="collector service stopped; closes after the next full source readiness",
            )
        await self._sources.stop()
        await asyncio.gather(*background, return_exceptions=True)
        await self._writers.stop()
        if failure is not None:
            raise failure

    async def _storage_loop(self) -> None:
        while not self._stop.is_set():
            async with self._operation_lock:
                self._sources.raise_if_failed()
                removed = await asyncio.to_thread(self._spool.apply_acks)
                status = await asyncio.to_thread(self._spool.status)
                if removed:
                    logger.info("garbage-collected ACKed chunks count=%d", removed)
                if status.hard_limited and self._storage_gap_id is None:
                    self._storage_gap_id = await self._gaps.open(
                        GapReason.STORAGE_EXHAUSTED,
                        exchange_symbols=self._universe_store.active.members,
                        detail=(
                            f"spool_bytes={status.used_bytes} free_bytes={status.free_bytes}"
                        ),
                    )
                    await self._sources.stop()
                elif not status.hard_limited and self._storage_gap_id is not None:
                    await self._sources.start(self._universe_store.active.members)
                    await self._sources.wait_ready()
                    await self._close_stale_gaps()
                    await self._seal_completed_days()
                    await self._gaps.close(
                        self._storage_gap_id,
                        GapReason.STORAGE_EXHAUSTED,
                        exchange_symbols=self._universe_store.active.members,
                    )
                    self._storage_gap_id = None
            await _wait_event(self._stop, self._config.storage_check_seconds)

    async def _midnight_loop(self) -> None:
        while not self._stop.is_set():
            now = datetime.now(UTC)
            midnight = datetime.combine(now.date() + timedelta(days=1), datetime.min.time(), UTC)
            pre_boundary = max(0.0, (midnight - now).total_seconds() - 2.0)
            await _wait_event(self._stop, pre_boundary)
            if self._stop.is_set():
                return
            async with self._operation_lock:
                was_running = self._sources.running
                boundary_gap = await self._gaps.open(
                    GapReason.PLANNED_BOUNDARY,
                    exchange_symbols=self._universe_store.active.members,
                    detail="UTC midnight writer and universe boundary",
                )
                await self._sources.stop()
                await _sleep_until(midnight)
                previous_date = midnight.date() - timedelta(days=1)
                await self._gaps.rollover(midnight)
                control = self._universe_store.apply_due(
                    midnight,
                    reasons=frozenset({ControlReason.DAILY, ControlReason.CANARY_SCALE}),
                )
                active = control or self._universe_store.active
                await self._ingest.rotate(universe_hash=active.universe_hash)
                self._gaps.set_universe_hash(active.universe_hash)
                await self._day_index.seal(previous_date, sealed_at=midnight)
                status = await asyncio.to_thread(self._spool.status)
                if was_running and not status.hard_limited:
                    await self._sources.start(active.members)
                    await self._sources.wait_ready()
                await self._gaps.close(
                    boundary_gap,
                    GapReason.PLANNED_BOUNDARY,
                    exchange_symbols=active.members,
                    detail="all sources ready after UTC boundary",
                )

    async def _control_loop(self) -> None:
        while not self._stop.is_set():
            async with self._operation_lock:
                control = self._universe_store.apply_due(
                    reasons=frozenset({ControlReason.NEW_LISTING_PROBE})
                )
                if control is not None:
                    was_running = self._sources.running
                    boundary_gap = await self._gaps.open(
                        GapReason.PLANNED_BOUNDARY,
                        exchange_symbols=self._universe_store.active.members,
                        detail="new-listing probe universe boundary",
                    )
                    await self._sources.stop()
                    await self._ingest.rotate(universe_hash=control.universe_hash)
                    self._gaps.set_universe_hash(control.universe_hash)
                    status = await asyncio.to_thread(self._spool.status)
                    if was_running and not status.hard_limited:
                        await self._sources.start(control.members)
                        await self._sources.wait_ready()
                    await self._gaps.close(
                        boundary_gap,
                        GapReason.PLANNED_BOUNDARY,
                        exchange_symbols=control.members,
                        detail="all sources ready after probe boundary",
                    )
            await _wait_event(self._stop, self._config.control_poll_seconds)

    async def _stats_loop(self) -> None:
        loop = asyncio.get_running_loop()
        previous_tick = loop.time()
        while not self._stop.is_set():
            status = await asyncio.to_thread(self._spool.status)
            usage = resource.getrusage(resource.RUSAGE_SELF)
            current_cpu = _host_cpu_sample()
            steal_ratio = _steal_ratio(self._previous_cpu, current_cpu)
            self._previous_cpu = current_cpu
            writer = self._writers.metrics
            now = loop.time()
            elapsed = max(now - previous_tick, 0.001)
            write_bytes_per_second = (
                writer.compressed_bytes - self._previous_written_bytes
            ) / elapsed
            event_loop_lag = max(0.0, elapsed - 60.0) if previous_tick else 0.0
            self._previous_written_bytes = writer.compressed_bytes
            previous_tick = now
            logger.info(
                "collector status queue_bytes=%d queue_ratio=%.3f spool_bytes=%d "
                "free_bytes=%d rss_bytes=%d arrow_bytes=%d cpu_user_s=%.3f cpu_system_s=%.3f "
                "cpu_steal_ratio=%.4f event_loop_lag_s=%.6f chunks=%d events=%d "
                "compressed_bytes=%d write_bytes_s=%.1f max_finalize_s=%.6f",
                self._queues.used_bytes,
                self._queues.utilization,
                status.used_bytes,
                status.free_bytes,
                _current_rss_bytes(),
                pa.total_allocated_bytes(),
                usage.ru_utime,
                usage.ru_stime,
                steal_ratio,
                event_loop_lag,
                writer.chunks,
                writer.events,
                writer.compressed_bytes,
                write_bytes_per_second,
                writer.max_finalize_seconds,
            )
            await _wait_event(self._stop, 60)

    def _quarantine_incomplete_chunks(self) -> None:
        writing_root = self._config.data_root / "writing"
        partials = list(writing_root.rglob("*.partial")) if writing_root.exists() else []
        if not partials:
            return
        quarantine = self._config.data_root / "control" / "quarantine" / self._boot_id
        quarantine.mkdir(parents=True, exist_ok=True)
        for path in partials:
            shutil.move(path, quarantine / path.name)
        logger.error("quarantined incomplete chunks count=%d", len(partials))

    async def _close_stale_gaps(self) -> None:
        stale = self._stale_gaps
        self._stale_gaps = ()
        for gap in stale:
            if gap.gap_id == self._storage_gap_id:
                continue
            await self._gaps.close(
                gap.gap_id,
                gap.reason,
                connection_id=gap.connection_id,
                exchange_symbols=gap.exchange_symbols,
                stream_types=gap.stream_types,
                detail="new collector boot reached full source readiness",
            )

    async def _seal_completed_days(self) -> None:
        now = datetime.now(UTC)
        for utc_date in self._day_index.completed_dates(now.date()):
            await self._day_index.seal(utc_date, sealed_at=now)


async def _sleep_until(moment: datetime) -> None:
    delay = (moment - datetime.now(UTC)).total_seconds()
    if delay > 0:
        await asyncio.sleep(delay)


async def _wait_event(event: asyncio.Event, delay_seconds: float) -> None:
    try:
        await asyncio.wait_for(event.wait(), timeout=delay_seconds)
    except TimeoutError:
        pass


def _current_rss_bytes() -> int:
    try:
        fields = Path("/proc/self/statm").read_text().split()
        return int(fields[1]) * os.sysconf("SC_PAGE_SIZE")
    except (FileNotFoundError, IndexError, ValueError):
        return 0


def _host_cpu_sample() -> tuple[int, int]:
    try:
        fields = Path("/proc/stat").read_text().splitlines()[0].split()[1:]
        values = [int(value) for value in fields]
    except (FileNotFoundError, IndexError, ValueError):
        return 0, 0
    total = sum(values)
    steal = values[7] if len(values) > 7 else 0
    return total, steal


def _steal_ratio(previous: tuple[int, int], current: tuple[int, int]) -> float:
    total = current[0] - previous[0]
    steal = current[1] - previous[1]
    return max(0.0, steal / total) if total > 0 else 0.0
