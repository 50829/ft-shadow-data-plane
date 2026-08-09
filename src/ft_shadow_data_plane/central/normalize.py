from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ft_shadow_data_plane.central.binance import logical_identity, parse_typed_row
from ft_shadow_data_plane.contracts.models import ChunkManifestV1, ContentType, DayManifestV1
from ft_shadow_data_plane.contracts.schema import RAW_EVENT_SCHEMA_V1
from ft_shadow_data_plane.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    fsync_directory,
    sha256_file,
)
from ft_shadow_data_plane.contracts.typed_schema import TYPED_EVENT_SCHEMA_V1


@dataclass(frozen=True, slots=True)
class NormalizeResult:
    raw_events: int
    typed_events: int
    duplicate_events: int
    output_files: int


class DayNormalizer:
    def __init__(
        self,
        *,
        raw_root: Path,
        derived_root: Path,
        collector_id: str,
        utc_date: date,
    ) -> None:
        self._collector_root = raw_root / f"collector={collector_id}"
        self._output_root = (
            derived_root
            / "typed"
            / f"collector={collector_id}"
            / f"date={utc_date.isoformat()}"
        )
        self._utc_date = utc_date
        self._collector_id = collector_id

    def run(self) -> NormalizeResult:
        day_manifest = self._load_day_manifest()
        chunk_manifests = self._load_chunk_manifests(day_manifest)
        seen: set[tuple[object, ...]] = set()
        raw_count = 0
        typed_count = 0
        duplicate_count = 0
        output_count = 0

        for manifest in sorted(
            chunk_manifests,
            key=lambda item: (item.min_app_receive_realtime_ns, item.chunk_id),
        ):
            if manifest.content_type is not ContentType.PARQUET:
                continue
            raw_path = self._collector_root / manifest.data_path
            self._verify_chunk(raw_path, manifest)
            output_path = self._output_root / f"{manifest.chunk_id}.typed.parquet"
            rows: list[dict[str, Any]] = []
            parquet = pq.ParquetFile(raw_path)
            if not parquet.schema_arrow.remove_metadata().equals(RAW_EVENT_SCHEMA_V1):
                raise ValueError(f"raw schema mismatch: {raw_path}")
            for batch in parquet.iter_batches(batch_size=10_000):
                for raw_row in batch.to_pylist():
                    raw_count += 1
                    typed = parse_typed_row(raw_row)
                    if typed is None:
                        continue
                    identity = logical_identity(typed)
                    if identity is not None:
                        typed["is_duplicate"] = identity in seen
                        if typed["is_duplicate"]:
                            duplicate_count += 1
                        else:
                            seen.add(identity)
                    rows.append(typed)
                    typed_count += 1
            if not rows:
                continue
            self._write_typed(output_path, rows)
            output_count += 1

        result = NormalizeResult(raw_count, typed_count, duplicate_count, output_count)
        marker = {
            "schema_version": 1,
            "collector_id": self._collector_id,
            "utc_date": self._utc_date.isoformat(),
            "raw_events": result.raw_events,
            "typed_events": result.typed_events,
            "duplicate_events": result.duplicate_events,
            "output_files": result.output_files,
        }
        atomic_write_bytes(self._output_root / "_NORMALIZED.json", canonical_json_bytes(marker))
        return result

    def _load_day_manifest(self) -> DayManifestV1:
        path = (
            self._collector_root
            / "day-manifests"
            / f"date={self._utc_date.isoformat()}"
            / "SEALED.json"
        )
        if not path.exists():
            raise FileNotFoundError(f"sealed day manifest does not exist: {path}")
        manifest = DayManifestV1.model_validate_json(path.read_bytes())
        if manifest.collector_id != self._collector_id or manifest.utc_date != self._utc_date:
            raise ValueError("sealed day manifest identity mismatch")
        return manifest

    def _load_chunk_manifests(
        self, day_manifest: DayManifestV1
    ) -> list[ChunkManifestV1]:
        manifests = []
        for chunk in day_manifest.chunks:
            data_path = self._collector_root / chunk.data_path
            manifest_path = data_path.with_suffix(".manifest.json")
            manifest = ChunkManifestV1.model_validate_json(manifest_path.read_bytes())
            if manifest.as_ref() != chunk:
                raise ValueError(f"day/chunk manifest disagreement: {manifest_path}")
            manifests.append(manifest)
        return manifests

    @staticmethod
    def _verify_chunk(path: Path, manifest: ChunkManifestV1) -> None:
        if path.stat().st_size != manifest.size_bytes or sha256_file(path) != manifest.sha256:
            raise ValueError(f"raw chunk integrity failure: {path}")

    @staticmethod
    def _write_typed(path: Path, rows: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_name(f".{path.name}.partial")
        table = pa.Table.from_pylist(rows, schema=TYPED_EVENT_SCHEMA_V1)
        pq.write_table(table, partial, compression="zstd", compression_level=3)
        with partial.open("rb") as source:
            os.fsync(source.fileno())
        partial.replace(path)
        fsync_directory(path.parent)
