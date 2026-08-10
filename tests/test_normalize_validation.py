from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from ft_shadow_data_plane.central.normalize import DayNormalizer
from ft_shadow_data_plane.contracts.data_contract import data_contract_hash_v1
from ft_shadow_data_plane.contracts.models import (
    ChunkManifestV1,
    ContentType,
    DayManifestV1,
    RawEventV1,
    StreamType,
    WriterGroup,
)
from ft_shadow_data_plane.contracts.schema import raw_events_to_table
from ft_shadow_data_plane.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    sha256_file,
)

UTC_DATE = date(2026, 8, 10)
RECEIVED_NS = int(datetime(2026, 8, 10, 12, tzinfo=UTC).timestamp() * 1_000_000_000)
UNIVERSE_HASH = "a" * 64
CONTRACT_HASH = data_contract_hash_v1()


def test_normalizer_accepts_manifest_that_matches_raw_chunk(tmp_path: Path) -> None:
    raw_root, derived_root = _write_day(tmp_path)

    result = DayNormalizer(
        raw_root=raw_root,
        derived_root=derived_root,
        collector_id="tokyo01",
        utc_date=UTC_DATE,
    ).run()

    assert result.raw_events == 1
    assert result.typed_events == 1


def test_normalizer_rejects_manifest_event_count_mismatch(tmp_path: Path) -> None:
    raw_root, derived_root = _write_day(tmp_path, manifest_event_count=2)

    with pytest.raises(ValueError, match="raw event count mismatch"):
        DayNormalizer(
            raw_root=raw_root,
            derived_root=derived_root,
            collector_id="tokyo01",
            utc_date=UTC_DATE,
        ).run()


def test_normalizer_rejects_parquet_metadata_mismatch(tmp_path: Path) -> None:
    raw_root, derived_root = _write_day(tmp_path, parquet_universe_hash="b" * 64)

    with pytest.raises(ValueError, match=r"raw metadata mismatch.*universe_hash"):
        DayNormalizer(
            raw_root=raw_root,
            derived_root=derived_root,
            collector_id="tokyo01",
            utc_date=UTC_DATE,
        ).run()


def _write_day(
    tmp_path: Path,
    *,
    manifest_event_count: int = 1,
    parquet_universe_hash: str = UNIVERSE_HASH,
) -> tuple[Path, Path]:
    raw_root = tmp_path / "raw"
    derived_root = tmp_path / "derived"
    collector_root = raw_root / "collector=tokyo01"
    relative = Path("date=2026-08-10/writer=trades_market/chunk-test.parquet")
    raw_path = collector_root / relative
    raw_path.parent.mkdir(parents=True)
    event = RawEventV1(
        schema_version=1,
        exchange_symbol="BTCUSDT",
        stream_type=StreamType.AGG_TRADE,
        collector_id="tokyo01",
        boot_id="boot",
        segment_id="segment",
        connection_id="connection",
        receive_seq=1,
        app_receive_realtime_ns=RECEIVED_NS,
        app_receive_monotonic_ns=1,
        payload_bytes=(
            b'{"e":"aggTrade","E":1,"T":2,"s":"BTCUSDT","a":3,'
            b'"p":"100","q":"1","f":4,"l":5,"m":true}'
        ),
    )
    metadata = {
        b"chunk_id": b"chunk-test",
        b"collector_id": b"tokyo01",
        b"data_contract_hash": CONTRACT_HASH.encode(),
        b"universe_hash": parquet_universe_hash.encode(),
        b"utc_date": UTC_DATE.isoformat().encode(),
        b"writer_group": WriterGroup.TRADES_MARKET.value.encode(),
    }
    pq.write_table(raw_events_to_table([event]).replace_schema_metadata(metadata), raw_path)
    manifest = ChunkManifestV1(
        chunk_id="chunk-test",
        data_path=relative.as_posix(),
        sha256=sha256_file(raw_path),
        size_bytes=raw_path.stat().st_size,
        content_type=ContentType.PARQUET,
        collector_id="tokyo01",
        writer_group=WriterGroup.TRADES_MARKET,
        utc_date=UTC_DATE,
        event_count=manifest_event_count,
        min_app_receive_realtime_ns=RECEIVED_NS,
        max_app_receive_realtime_ns=RECEIVED_NS,
        data_contract_hash=CONTRACT_HASH,
        universe_hash=UNIVERSE_HASH,
        created_at=datetime(2026, 8, 10, 12, tzinfo=UTC),
    )
    atomic_write_bytes(raw_path.with_suffix(".manifest.json"), canonical_json_bytes(manifest))
    day = DayManifestV1(
        collector_id="tokyo01",
        utc_date=UTC_DATE,
        sealed_at=datetime(2026, 8, 11, tzinfo=UTC),
        chunks=(manifest.as_ref(),),
    )
    atomic_write_bytes(
        collector_root / "day-manifests/date=2026-08-10/SEALED.json",
        canonical_json_bytes(day),
    )
    return raw_root, derived_root
