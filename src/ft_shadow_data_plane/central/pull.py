from __future__ import annotations

import logging
import os
import posixpath
import shlex
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Protocol

from ft_shadow_data_plane.central.config import CentralConfig
from ft_shadow_data_plane.contracts.models import AckV1, ChunkManifestV1, DayManifestV1
from ft_shadow_data_plane.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    fsync_directory,
    sha256_file,
)

logger = logging.getLogger(__name__)


class RemoteStore(Protocol):
    def list_files(self, root: str) -> tuple[str, ...]: ...

    def read_bytes(self, path: str) -> bytes: ...

    def download(self, remote_path: str, local_file: BinaryIO) -> None: ...

    def write_atomic(self, path: str, content: bytes) -> None: ...


class FilesystemRemoteStore:
    """Expose an rsync mirror through the same durable ingest contract used by tests."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def list_files(self, root: str) -> tuple[str, ...]:
        directory = self._path(root)
        if not directory.exists():
            return ()
        files = (
            path.relative_to(self._root).as_posix()
            for path in directory.rglob("*")
            if path.is_file()
        )
        return tuple(
            sorted(files)
        )

    def read_bytes(self, path: str) -> bytes:
        return self._path(path).read_bytes()

    def download(self, remote_path: str, local_file: BinaryIO) -> None:
        with self._path(remote_path).open("rb") as source:
            shutil.copyfileobj(source, local_file, length=1024 * 1024)

    def write_atomic(self, path: str, content: bytes) -> None:
        atomic_write_bytes(self._path(path), content)

    def _path(self, value: str) -> Path:
        relative = safe_remote_root(value)
        path = self._root / relative
        if not path.resolve(strict=False).is_relative_to(self._root.resolve()):
            raise ValueError("rsync mirror path escapes its root")
        return path


class RsyncTransport:
    def __init__(self, config: CentralConfig) -> None:
        self._config = config
        self._ready_root = safe_remote_root(config.remote_ready_root)
        self._ack_root = safe_remote_root(config.remote_ack_root)
        self._mirror = config.local_staging_root

    @property
    def store(self) -> FilesystemRemoteStore:
        return FilesystemRemoteStore(self._mirror)

    def pull_ready(self) -> None:
        destination = self._mirror / self._ready_root
        destination.mkdir(parents=True, exist_ok=True)
        self._run(
            "--archive",
            "--delete-delay",
            "--delay-updates",
            "--no-links",
            "--partial",
            self._remote(f"{self._ready_root}/"),
            f"{destination}/",
        )

    def push_acks(self) -> None:
        source = self._mirror / self._ack_root
        source.mkdir(parents=True, exist_ok=True)
        if not any(source.glob("*.ack.json")):
            return
        self._run(
            "--archive",
            "--delay-updates",
            f"{source}/",
            self._remote(f"{self._ack_root}/"),
        )
        for path in source.glob("*.ack.json"):
            path.unlink()
        fsync_directory(source)

    def _run(self, *arguments: str) -> None:
        command = [
            str(self._config.rsync_binary),
            "--timeout",
            str(self._config.io_timeout_seconds),
            "--rsh",
            shlex.join(self._ssh_command()),
            *arguments,
        ]
        result = subprocess.run(command, check=False, capture_output=True, text=True)
        if result.returncode != 0:
            raise OSError(
                f"rsync failed exit={result.returncode}: {result.stderr.strip()}"
            )

    def _ssh_command(self) -> list[str]:
        return [
            str(self._config.ssh_binary),
            "-p",
            str(self._config.port),
            "-i",
            str(self._config.client_key),
            "-o",
            "BatchMode=yes",
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            f"UserKnownHostsFile={self._config.known_hosts}",
            "-o",
            f"ConnectTimeout={self._config.connect_timeout_seconds}",
        ]

    def _remote(self, path: str) -> str:
        return f"{self._config.username}@{self._config.host}:{path}"


class CentralPuller:
    def __init__(
        self,
        remote: RemoteStore,
        *,
        remote_ready_root: str,
        remote_ack_root: str,
        local_raw_root: Path,
    ) -> None:
        self._remote = remote
        self._remote_ready_root = remote_ready_root.rstrip("/")
        self._remote_ack_root = remote_ack_root.rstrip("/")
        self._local_raw_root = local_raw_root

    def run(self) -> tuple[int, int]:
        files = self._remote.list_files(self._remote_ready_root)
        chunk_manifests = [path for path in files if path.endswith(".manifest.json")]
        day_manifests = [path for path in files if path.endswith("/SEALED.json")]
        pulled = 0
        failures = 0
        for remote_manifest_path in chunk_manifests:
            try:
                if self._ingest_chunk(remote_manifest_path):
                    pulled += 1
            except (OSError, ValueError):
                failures += 1
                logger.exception("chunk ingest failed remote_path=%s", remote_manifest_path)
        for remote_day_path in day_manifests:
            try:
                self._publish_complete_day(remote_day_path)
            except (OSError, ValueError):
                failures += 1
                logger.exception("day manifest ingest failed remote_path=%s", remote_day_path)
        return pulled, failures

    def _ingest_chunk(self, remote_manifest_path: str) -> bool:
        manifest_bytes = self._remote.read_bytes(remote_manifest_path)
        manifest = ChunkManifestV1.model_validate_json(manifest_bytes)
        collector_root = self._local_raw_root / f"collector={manifest.collector_id}"
        destination = collector_root / manifest.data_path
        if destination.exists():
            self._verify_local(destination, manifest)
            wrote_data = False
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            partial = destination.with_name(f".{destination.name}.partial")
            with partial.open("wb") as output:
                self._remote.download(
                    posixpath.join(self._remote_ready_root, manifest.data_path), output
                )
                output.flush()
                os.fsync(output.fileno())
            self._verify_local(partial, manifest)
            partial.replace(destination)
            fsync_directory(destination.parent)
            wrote_data = True

        local_manifest = destination.with_suffix(".manifest.json")
        if local_manifest.exists() and local_manifest.read_bytes() != manifest_bytes:
            raise ValueError(f"conflicting local manifest: {local_manifest}")
        if not local_manifest.exists():
            atomic_write_bytes(local_manifest, manifest_bytes)

        ack = AckV1(
            chunk_id=manifest.chunk_id,
            sha256=manifest.sha256,
            durable_at=datetime.now(UTC),
        )
        ack_path = posixpath.join(self._remote_ack_root, f"{manifest.chunk_id}.ack.json")
        self._remote.write_atomic(ack_path, canonical_json_bytes(ack))
        return wrote_data

    def _publish_complete_day(self, remote_day_path: str) -> bool:
        content = self._remote.read_bytes(remote_day_path)
        manifest = DayManifestV1.model_validate_json(content)
        collector_root = self._local_raw_root / f"collector={manifest.collector_id}"
        destination = (
            collector_root
            / "day-manifests"
            / f"date={manifest.utc_date.isoformat()}"
            / "SEALED.json"
        )
        if destination.exists():
            if destination.read_bytes() != content:
                raise ValueError(f"conflicting sealed day manifest: {destination}")
            return True
        for chunk in manifest.chunks:
            path = collector_root / chunk.data_path
            if not path.exists() or path.stat().st_size != chunk.size_bytes:
                return False
            if sha256_file(path) != chunk.sha256:
                raise ValueError(f"day manifest hash mismatch: {path}")
        atomic_write_bytes(destination, content)
        return True

    @staticmethod
    def _verify_local(path: Path, manifest: ChunkManifestV1) -> None:
        if path.stat().st_size != manifest.size_bytes:
            raise ValueError(f"chunk size mismatch: {path}")
        if sha256_file(path) != manifest.sha256:
            raise ValueError(f"chunk hash mismatch: {path}")


def safe_remote_root(value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("remote paths must be relative to the restricted rsync root")
    return value.rstrip("/")
