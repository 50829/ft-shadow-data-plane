from __future__ import annotations

from datetime import date
from pathlib import Path

from ft_shadow_data_plane.contracts.models import ContentType, DayManifestV1, GapEventV1
from ft_shadow_data_plane.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    sha256_file,
)


def build_transport_gap_ledger(
    *,
    raw_root: Path,
    derived_root: Path,
    collector_id: str,
    utc_date: date,
) -> Path:
    collector_root = raw_root / f"collector={collector_id}"
    day_path = collector_root / "day-manifests" / f"date={utc_date.isoformat()}" / "SEALED.json"
    day = DayManifestV1.model_validate_json(day_path.read_bytes())
    events: list[GapEventV1] = []
    for chunk in day.chunks:
        if chunk.content_type is not ContentType.GAP_JSON:
            continue
        path = collector_root / chunk.data_path
        if path.stat().st_size != chunk.size_bytes or sha256_file(path) != chunk.sha256:
            raise ValueError(f"gap artifact integrity failure: {path}")
        events.append(GapEventV1.model_validate_json(path.read_bytes()))
    events.sort(key=_gap_sort_key)
    destination = (
        derived_root
        / "quality"
        / f"collector={collector_id}"
        / f"date={utc_date.isoformat()}"
        / "transport-gaps.jsonl"
    )
    content = b"".join(canonical_json_bytes(event) for event in events)
    atomic_write_bytes(destination, content)
    return destination


def _gap_sort_key(event: GapEventV1) -> tuple[int, str, str]:
    effective_ns = (
        event.affected_from_realtime_ns
        if event.state.value == "OPEN" and event.affected_from_realtime_ns is not None
        else event.observed_at_realtime_ns
    )
    return effective_ns, event.gap_id, event.state.value
