from __future__ import annotations

import os
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from uuid import uuid4

from ft_shadow_data_plane.contracts.models import (
    ChunkManifestV1,
    ContentType,
    GapEventV1,
    GapReason,
    GapState,
    StreamType,
    WriterGroup,
)
from ft_shadow_data_plane.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    fsync_directory,
    sha256_file,
)
from ft_shadow_data_plane.edge.day_index import DayIndex


class GapJournal:
    """Publish gap facts without using the market-data queue."""

    def __init__(
        self,
        data_root: Path,
        *,
        collector_id: str,
        data_contract_hash: str,
        universe_hash: str,
        day_index: DayIndex,
        reserve_bytes: int = 1024 * 1024,
    ) -> None:
        self._data_root = data_root
        self._collector_id = collector_id
        self._data_contract_hash = data_contract_hash
        self._universe_hash = universe_hash
        self._day_index = day_index
        self._reserve_path = data_root / "control" / "gap-journal.reserve"
        self._open_root = data_root / "control" / "open-gaps"
        self._reserve_bytes = reserve_bytes

    def initialize(self) -> None:
        self._reserve_path.parent.mkdir(parents=True, exist_ok=True)
        self._open_root.mkdir(parents=True, exist_ok=True)
        if self._reserve_path.exists():
            return
        descriptor = os.open(self._reserve_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            if hasattr(os, "posix_fallocate"):
                os.posix_fallocate(descriptor, 0, self._reserve_bytes)
            else:
                os.ftruncate(descriptor, self._reserve_bytes)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def set_universe_hash(self, value: str) -> None:
        self._universe_hash = value

    async def open(
        self,
        reason: GapReason,
        *,
        connection_id: str | None = None,
        exchange_symbols: tuple[str, ...] = (),
        stream_types: tuple[StreamType, ...] = (),
        detail: str = "",
    ) -> str:
        gap_id = f"gap-{uuid4().hex}"
        event = GapEventV1(
            gap_id=gap_id,
            state=GapState.OPEN,
            reason=reason,
            collector_id=self._collector_id,
            connection_id=connection_id,
            exchange_symbols=exchange_symbols,
            stream_types=stream_types,
            observed_at_realtime_ns=time_ns(),
            detail=detail,
        )
        await self._publish(event)
        atomic_write_bytes(
            self._open_root / f"{gap_id}.json", canonical_json_bytes(event)
        )
        return gap_id

    async def close(
        self,
        gap_id: str,
        reason: GapReason,
        *,
        connection_id: str | None = None,
        exchange_symbols: tuple[str, ...] = (),
        stream_types: tuple[StreamType, ...] = (),
        detail: str = "",
    ) -> None:
        observed_at = time_ns()
        state_path = self._open_root / f"{gap_id}.json"
        if state_path.exists():
            closed_at = datetime.fromtimestamp(observed_at // 1_000_000_000, tz=UTC)
            await self._roll_state_to(state_path, closed_at.date())
        await self._publish(
            GapEventV1(
                gap_id=gap_id,
                state=GapState.CLOSED,
                reason=reason,
                collector_id=self._collector_id,
                connection_id=connection_id,
                exchange_symbols=exchange_symbols,
                stream_types=stream_types,
                observed_at_realtime_ns=observed_at,
                detail=detail,
            )
        )
        state_path.unlink(missing_ok=True)
        fsync_directory(self._open_root)

    def stale_open_events(self) -> tuple[GapEventV1, ...]:
        return tuple(
            GapEventV1.model_validate_json(path.read_bytes())
            for path in sorted(self._open_root.glob("*.json"))
        )

    async def rollover(self, boundary: datetime) -> None:
        if boundary.tzinfo is None or boundary.utcoffset() != UTC.utcoffset(boundary):
            raise ValueError("gap rollover boundary must be UTC")
        if any((boundary.hour, boundary.minute, boundary.second, boundary.microsecond)):
            raise ValueError("gap rollover boundary must be UTC midnight")
        for state_path in sorted(self._open_root.glob("*.json")):
            await self._roll_state_to(state_path, boundary.date())

    async def _publish(self, event: GapEventV1) -> None:
        try:
            manifest = self._write(event)
        except OSError:
            self._reserve_path.unlink(missing_ok=True)
            manifest = self._write(event)
        await self._day_index.record(manifest.utc_date, manifest.as_ref())

    async def _roll_state_to(self, state_path: Path, target_date: date) -> None:
        opened = _read_gap_event(state_path)
        opened_at = datetime.fromtimestamp(
            opened.observed_at_realtime_ns // 1_000_000_000, tz=UTC
        )
        current_date = opened_at.date() + timedelta(days=1)
        while current_date <= target_date:
            day_start = datetime.combine(current_date, time.min, UTC)
            previous_day_end_ns = int(day_start.timestamp() * 1_000_000_000) - 1
            await self._publish(
                opened.model_copy(
                    update={
                        "state": GapState.CLOSED,
                        "observed_at_realtime_ns": previous_day_end_ns,
                        "detail": "gap continues into the next UTC day",
                    }
                )
            )
            opened = opened.model_copy(
                update={
                    "state": GapState.OPEN,
                    "observed_at_realtime_ns": int(day_start.timestamp() * 1_000_000_000),
                    "detail": "gap continued from a previous UTC day",
                }
            )
            await self._publish(opened)
            atomic_write_bytes(state_path, canonical_json_bytes(opened))
            current_date += timedelta(days=1)

    def _write(self, event: GapEventV1) -> ChunkManifestV1:
        observed = datetime.fromtimestamp(
            event.observed_at_realtime_ns // 1_000_000_000, tz=UTC
        )
        artifact_id = (
            f"{event.gap_id}-{event.observed_at_realtime_ns}-{event.state.value.lower()}"
        )
        relative = (
            Path(f"date={observed.date().isoformat()}")
            / "writer=control"
            / f"{artifact_id}.gap.json"
        )
        destination = self._data_root / "ready" / relative
        content = canonical_json_bytes(event)
        atomic_write_bytes(destination, content)
        manifest = ChunkManifestV1(
            chunk_id=artifact_id,
            data_path=relative.as_posix(),
            sha256=sha256_file(destination),
            size_bytes=destination.stat().st_size,
            content_type=ContentType.GAP_JSON,
            collector_id=self._collector_id,
            writer_group=WriterGroup.CONTROL,
            utc_date=observed.date(),
            event_count=1,
            min_app_receive_realtime_ns=event.observed_at_realtime_ns,
            max_app_receive_realtime_ns=event.observed_at_realtime_ns,
            data_contract_hash=self._data_contract_hash,
            universe_hash=self._universe_hash,
            created_at=datetime.now(UTC),
        )
        atomic_write_bytes(
            destination.with_suffix(".manifest.json"), canonical_json_bytes(manifest)
        )
        return manifest


def time_ns() -> int:
    import time

    return time.time_ns()


def _read_gap_event(path: Path) -> GapEventV1:
    return GapEventV1.model_validate_json(path.read_bytes())
