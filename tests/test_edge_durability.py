from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pytest

from ft_shadow_data_plane.contracts.models import (
    AckV1,
    DayManifestV1,
    GapEventV1,
    GapReason,
    GapState,
    RawEventV1,
    StreamType,
    WriterGroup,
)
from ft_shadow_data_plane.contracts.serde import atomic_write_bytes, canonical_json_bytes
from ft_shadow_data_plane.edge.day_index import DayIndex
from ft_shadow_data_plane.edge.gaps import GapJournal
from ft_shadow_data_plane.edge.ingest import IngestCoordinator
from ft_shadow_data_plane.edge.queue import ByteBoundedQueues, QueueOverloaded
from ft_shadow_data_plane.edge.spool import SpoolManager
from ft_shadow_data_plane.edge.writer import ChunkLimits, WriterPool

HASH_A = "a" * 64
HASH_B = "b" * 64


@pytest.mark.asyncio
async def test_queue_boundary_rotation_and_exact_ack(tmp_path: Path) -> None:
    queues = ByteBoundedQueues(2_000, warn_ratio=0.7, resume_ratio=0.5)
    day_index = DayIndex(tmp_path, "tokyo01")
    writers = WriterPool(
        tmp_path,
        collector_id="tokyo01",
        data_contract_hash=HASH_A,
        universe_hash=HASH_A,
        queues=queues,
        day_index=day_index,
        limits=ChunkLimits(60, 1_000_000, 10_000, 1, 1),
    )
    writers.start()
    ingest = IngestCoordinator(queues, writers)
    await ingest.put(_event(1, b'{"first":1}'))
    await ingest.rotate(universe_hash=HASH_B)
    await ingest.put(_event(2, b'{"second":2}'))
    await writers.stop()

    manifests = sorted((tmp_path / "ready").rglob("*.manifest.json"))
    assert len(manifests) == 2
    parsed = [__import__("json").loads(path.read_bytes()) for path in manifests]
    assert {item["universe_hash"] for item in parsed} == {HASH_A, HASH_B}

    spool = SpoolManager(tmp_path, max_bytes=10**9, minimum_free_bytes=0)
    spool.initialize()
    first = parsed[0]
    wrong = AckV1(chunk_id=first["chunk_id"], sha256=HASH_A, durable_at=datetime.now(UTC))
    ack_path = tmp_path / "control/acks" / f"{first['chunk_id']}.ack.json"
    atomic_write_bytes(ack_path, canonical_json_bytes(wrong))
    assert spool.apply_acks() == 0
    assert (tmp_path / "ready" / first["data_path"]).exists()

    exact = AckV1(
        chunk_id=first["chunk_id"], sha256=first["sha256"], durable_at=datetime.now(UTC)
    )
    atomic_write_bytes(ack_path, canonical_json_bytes(exact))
    assert spool.apply_acks() == 1
    assert not (tmp_path / "ready" / first["data_path"]).exists()
    chunk_date = date.fromisoformat(first["utc_date"])
    sealed = await day_index.seal(
        chunk_date,
        sealed_at=datetime.combine(chunk_date + timedelta(days=1), time.min, UTC),
    )
    assert len(DayManifestV1.model_validate_json(sealed.read_bytes()).chunks) == 2
    assert not (tmp_path / "control/acked-manifests" / f"date={chunk_date}").exists()
    second = parsed[1]
    atomic_write_bytes(
        tmp_path / "control/acks" / f"{second['chunk_id']}.ack.json",
        canonical_json_bytes(
            AckV1(
                chunk_id=second["chunk_id"],
                sha256=second["sha256"],
                durable_at=datetime.now(UTC),
            )
        ),
    )
    assert spool.apply_acks() == 1
    assert not (tmp_path / "control/acked-manifests" / f"date={chunk_date}").exists()


@pytest.mark.asyncio
async def test_queue_hard_limit_rejects_without_silent_eviction() -> None:
    queues = ByteBoundedQueues(300, warn_ratio=0.7, resume_ratio=0.5)
    with pytest.raises(QueueOverloaded):
        await queues.put(_event(1, b"x" * 100))
    assert queues.used_bytes == 0


@pytest.mark.asyncio
async def test_queue_tracks_activity_per_writer_group(monkeypatch: pytest.MonkeyPatch) -> None:
    queues = ByteBoundedQueues(2_000, warn_ratio=0.7, resume_ratio=0.5)
    monkeypatch.setattr("ft_shadow_data_plane.edge.queue.time.monotonic", lambda: 100.0)

    await queues.put(_event(1, b'{"depth":1}'))

    assert queues.idle_seconds(WriterGroup.TRADES_MARKET, now=103.5) == 3.5
    assert queues.idle_seconds(WriterGroup.DEPTH, now=103.5) is None


def test_spool_skips_manifest_scan_when_there_are_no_acks(tmp_path: Path) -> None:
    spool = SpoolManager(tmp_path, max_bytes=10**9, minimum_free_bytes=0)
    spool.initialize()
    malformed = tmp_path / "ready/date=2026-08-10/writer=depth/bad.manifest.json"
    malformed.parent.mkdir(parents=True)
    malformed.write_bytes(b"not-json")

    assert spool.apply_acks() == 0


@pytest.mark.asyncio
async def test_open_gap_survives_restart_until_explicit_close(tmp_path: Path) -> None:
    day_index = DayIndex(tmp_path, "tokyo01")
    gaps = GapJournal(
        tmp_path,
        collector_id="tokyo01",
        data_contract_hash=HASH_A,
        universe_hash=HASH_A,
        day_index=day_index,
        reserve_bytes=4096,
    )
    gaps.initialize()
    opened_at = datetime(2026, 8, 10, 23, 55, tzinfo=UTC)
    closed_at = datetime(2026, 8, 13, 0, 5, tzinfo=UTC)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            "ft_shadow_data_plane.edge.gaps.time_ns",
            lambda: int(opened_at.timestamp() * 1_000_000_000),
        )
        gap_id = await gaps.open(
            GapReason.COLLECTOR_STOPPED, exchange_symbols=("BTCUSDT",)
        )
    await gaps.rollover(datetime(2026, 8, 11, tzinfo=UTC))
    restarted = GapJournal(
        tmp_path,
        collector_id="tokyo01",
        data_contract_hash=HASH_A,
        universe_hash=HASH_A,
        day_index=day_index,
        reserve_bytes=4096,
    )
    restarted.initialize()
    assert [event.gap_id for event in restarted.stale_open_events()] == [gap_id]
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            "ft_shadow_data_plane.edge.gaps.time_ns",
            lambda: int(closed_at.timestamp() * 1_000_000_000),
        )
        await restarted.close(
            gap_id, GapReason.COLLECTOR_STOPPED, exchange_symbols=("BTCUSDT",)
        )
    assert restarted.stale_open_events() == ()

    middle_date = date(2026, 8, 12)
    sealed = await day_index.seal(
        middle_date, sealed_at=datetime(2026, 8, 13, 0, 6, tzinfo=UTC)
    )
    manifest = DayManifestV1.model_validate_json(sealed.read_bytes())
    events = [
        GapEventV1.model_validate_json((tmp_path / "ready" / chunk.data_path).read_bytes())
        for chunk in manifest.chunks
    ]
    events.sort(key=lambda event: event.observed_at_realtime_ns)
    assert [event.state for event in events] == [GapState.OPEN, GapState.CLOSED]


def _event(sequence: int, payload: bytes) -> RawEventV1:
    return RawEventV1(
        schema_version=1,
        exchange_symbol="BTCUSDT",
        stream_type=StreamType.AGG_TRADE,
        collector_id="tokyo01",
        boot_id="boot",
        segment_id="segment",
        connection_id="connection",
        receive_seq=sequence,
        app_receive_realtime_ns=1_786_320_000_000_000_000 + sequence,
        app_receive_monotonic_ns=sequence,
        payload_bytes=payload,
    )
