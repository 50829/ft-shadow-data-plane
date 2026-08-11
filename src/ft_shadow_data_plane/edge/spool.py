from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from pydantic import TypeAdapter

from ft_shadow_data_plane.contracts.models import AckV1, ChunkManifestV1
from ft_shadow_data_plane.contracts.serde import fsync_directory

logger = logging.getLogger(__name__)
ACK_ADAPTER = TypeAdapter(AckV1)
MANIFEST_ADAPTER = TypeAdapter(ChunkManifestV1)


@dataclass(frozen=True, slots=True)
class SpoolStatus:
    used_bytes: int
    free_bytes: int
    hard_limited: bool


class SpoolManager:
    def __init__(self, data_root: Path, *, max_bytes: int, minimum_free_bytes: int) -> None:
        self.data_root = data_root
        self.ready_root = data_root / "ready"
        self.ack_root = data_root / "control" / "acks"
        self.max_bytes = max_bytes
        self.minimum_free_bytes = minimum_free_bytes

    def initialize(self) -> None:
        for path in (self.ready_root, self.ack_root, self.data_root / "writing"):
            path.mkdir(parents=True, exist_ok=True)

    def status(self) -> SpoolStatus:
        used = _tree_size(self.data_root / "ready") + _tree_size(self.data_root / "writing")
        free = shutil.disk_usage(self.data_root).free
        return SpoolStatus(
            used_bytes=used,
            free_bytes=free,
            hard_limited=used >= self.max_bytes or free < self.minimum_free_bytes,
        )

    def apply_acks(self) -> int:
        ack_paths = sorted(self.ack_root.glob("*.ack.json"))
        if not ack_paths:
            return 0
        manifests = {
            manifest.chunk_id: (path, manifest)
            for path in self.ready_root.rglob("*.manifest.json")
            for manifest in (MANIFEST_ADAPTER.validate_json(path.read_bytes()),)
        }
        removed = 0
        for ack_path in ack_paths:
            ack = ACK_ADAPTER.validate_json(ack_path.read_bytes())
            item = manifests.get(ack.chunk_id)
            if item is None:
                ack_path.unlink(missing_ok=True)
                fsync_directory(ack_path.parent)
                continue
            manifest_path, manifest = item
            if ack.sha256 != manifest.sha256:
                logger.error("refusing mismatched ACK for chunk_id=%s", ack.chunk_id)
                continue
            data_path = self.ready_root / manifest.data_path
            if data_path.exists():
                data_path.unlink()
                fsync_directory(data_path.parent)
            sealed = (
                self.ready_root
                / "day-manifests"
                / f"date={manifest.utc_date.isoformat()}"
                / "SEALED.json"
            )
            if sealed.exists():
                manifest_path.unlink()
                fsync_directory(manifest_path.parent)
            else:
                acked_manifest = (
                    self.data_root
                    / "control"
                    / "acked-manifests"
                    / f"date={manifest.utc_date.isoformat()}"
                    / manifest_path.name
                )
                acked_manifest.parent.mkdir(parents=True, exist_ok=True)
                manifest_path.replace(acked_manifest)
                fsync_directory(manifest_path.parent)
                fsync_directory(acked_manifest.parent)
            ack_path.unlink(missing_ok=True)
            fsync_directory(ack_path.parent)
            removed += 1
        return removed


def _tree_size(root: Path) -> int:
    if not root.exists():
        return 0
    total = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        if entry.is_symlink():
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            total += entry.stat(follow_symlinks=False).st_size
                    except FileNotFoundError:
                        continue
        except FileNotFoundError:
            continue
    return total
