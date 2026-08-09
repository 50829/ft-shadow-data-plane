from __future__ import annotations

import logging
import os
import posixpath
import stat
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Protocol, Self

import paramiko

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


class ParamikoRemoteStore(AbstractContextManager["ParamikoRemoteStore"]):
    def __init__(self, config: CentralConfig) -> None:
        self._config = config
        self._client: paramiko.SSHClient | None = None
        self._sftp: paramiko.SFTPClient | None = None

    def __enter__(self) -> Self:
        client = paramiko.SSHClient()
        client.load_host_keys(str(self._config.known_hosts))
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
        client.connect(
            hostname=self._config.host,
            port=self._config.port,
            username=self._config.username,
            key_filename=str(self._config.client_key),
            timeout=self._config.connect_timeout_seconds,
            allow_agent=False,
            look_for_keys=False,
        )
        self._client = client
        self._sftp = client.open_sftp()
        return self

    def __exit__(self, *args: object) -> None:
        if self._sftp is not None:
            self._sftp.close()
        if self._client is not None:
            self._client.close()

    @property
    def sftp(self) -> paramiko.SFTPClient:
        if self._sftp is None:
            raise RuntimeError("SFTP connection is not open")
        return self._sftp

    def list_files(self, root: str) -> tuple[str, ...]:
        files: list[str] = []
        pending = [root]
        while pending:
            directory = pending.pop()
            try:
                entries = self.sftp.listdir_attr(directory)
            except FileNotFoundError:
                continue
            for entry in entries:
                path = posixpath.join(directory, entry.filename)
                if stat.S_ISDIR(entry.st_mode):
                    pending.append(path)
                elif stat.S_ISREG(entry.st_mode):
                    files.append(path)
        return tuple(sorted(files))

    def read_bytes(self, path: str) -> bytes:
        with self.sftp.open(path, "rb") as source:
            value = source.read()
        return value if isinstance(value, bytes) else value.encode()

    def download(self, remote_path: str, local_file: BinaryIO) -> None:
        self.sftp.getfo(remote_path, local_file)

    def write_atomic(self, path: str, content: bytes) -> None:
        parent = posixpath.dirname(path)
        temporary = posixpath.join(parent, f".{posixpath.basename(path)}.partial")
        with self.sftp.open(temporary, "wb") as destination:
            destination.write(content)
            destination.flush()
        try:
            self.sftp.posix_rename(temporary, path)
        except OSError:
            self.sftp.rename(temporary, path)


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
        for chunk in manifest.chunks:
            path = collector_root / chunk.data_path
            if not path.exists() or path.stat().st_size != chunk.size_bytes:
                return False
            if sha256_file(path) != chunk.sha256:
                raise ValueError(f"day manifest hash mismatch: {path}")
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
        raise ValueError("remote paths must be relative to the SFTP account root")
    return value.rstrip("/")
