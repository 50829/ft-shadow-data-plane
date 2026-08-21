from __future__ import annotations

import subprocess
from datetime import UTC, date, datetime
from pathlib import Path
from typing import BinaryIO

import pytest

from ft_shadow_data_plane.central.config import CentralConfig
from ft_shadow_data_plane.central.pull import CentralPuller, RsyncTransport
from ft_shadow_data_plane.contracts.models import (
    ChunkManifestV1,
    ContentType,
    DayManifestV1,
    WriterGroup,
)
from ft_shadow_data_plane.contracts.serde import canonical_json_bytes, sha256_bytes

HASH = "a" * 64


class MemoryRemote:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = files
        self.writes: dict[str, bytes] = {}

    def list_files(self, root: str) -> tuple[str, ...]:
        return tuple(sorted(path for path in self.files if path.startswith(f"{root}/")))

    def read_bytes(self, path: str) -> bytes:
        return self.files[path]

    def download(self, remote_path: str, local_file: BinaryIO) -> None:
        local_file.write(self.files[remote_path])

    def write_atomic(self, path: str, content: bytes) -> None:
        self.writes[path] = content


def test_pull_is_durable_idempotent_and_publishes_complete_day(tmp_path: Path) -> None:
    remote, manifest = _remote_fixture(b"valid parquet stand-in")
    puller = CentralPuller(
        remote,
        remote_ready_root="ready",
        remote_ack_root="control/acks",
        local_raw_root=tmp_path,
    )
    assert puller.run() == (1, 0)
    assert puller.run() == (0, 0)
    assert f"control/acks/{manifest.chunk_id}.ack.json" in remote.writes
    assert (
        tmp_path
        / "collector=tokyo01/day-manifests/date=2026-08-10/SEALED.json"
    ).exists()


def test_hash_mismatch_never_acknowledges(tmp_path: Path) -> None:
    remote, manifest = _remote_fixture(b"expected")
    remote.files[f"ready/{manifest.data_path}"] = b"corrupted"
    puller = CentralPuller(
        remote,
        remote_ready_root="ready",
        remote_ack_root="control/acks",
        local_raw_root=tmp_path,
    )
    assert puller.run() == (0, 1)
    assert f"control/acks/{manifest.chunk_id}.ack.json" not in remote.writes


def test_matching_sealed_day_is_not_rehashed_on_every_pull(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    remote, _manifest = _remote_fixture(b"valid parquet stand-in")
    puller = CentralPuller(
        remote,
        remote_ready_root="ready",
        remote_ack_root="control/acks",
        local_raw_root=tmp_path,
    )
    assert puller.run() == (1, 0)
    remote.files = {
        path: content for path, content in remote.files.items() if path.endswith("/SEALED.json")
    }

    def unexpected_hash(_path: Path) -> str:
        raise AssertionError("an already-published sealed day must not be rehashed")

    monkeypatch.setattr("ft_shadow_data_plane.central.pull.sha256_file", unexpected_hash)

    assert puller.run() == (0, 0)


def test_rsync_transport_pins_ssh_identity_and_host_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    def record(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr("ft_shadow_data_plane.central.pull.subprocess.run", record)
    config = CentralConfig(
        host="167.179.115.243",
        username="data-puller",
        client_key=tmp_path / "key",
        known_hosts=tmp_path / "known_hosts",
        local_raw_root=tmp_path / "raw",
        local_staging_root=tmp_path / "staging",
    )
    transport = RsyncTransport(config)
    transport.pull_ready()
    ack = tmp_path / "staging/control/acks/chunk-test.ack.json"
    ack.parent.mkdir(parents=True)
    ack.write_text("{}", encoding="ascii")
    transport.push_acks()

    assert len(calls) == 2
    assert "StrictHostKeyChecking=yes" in calls[0][4]
    assert f"UserKnownHostsFile={config.known_hosts}" in calls[0][4]
    assert calls[0][-2] == "data-puller@167.179.115.243:ready/"
    assert calls[1][-1] == "data-puller@167.179.115.243:control/acks/"
    assert not ack.exists()


def _remote_fixture(data: bytes) -> tuple[MemoryRemote, ChunkManifestV1]:
    relative = "date=2026-08-10/writer=depth/chunk-test.parquet"
    manifest = ChunkManifestV1(
        chunk_id="chunk-test",
        data_path=relative,
        sha256=sha256_bytes(data),
        size_bytes=len(data),
        content_type=ContentType.PARQUET,
        collector_id="tokyo01",
        writer_group=WriterGroup.DEPTH,
        utc_date=date(2026, 8, 10),
        event_count=1,
        min_app_receive_realtime_ns=1,
        max_app_receive_realtime_ns=1,
        data_contract_hash=HASH,
        universe_hash=HASH,
        created_at=datetime(2026, 8, 10, tzinfo=UTC),
    )
    day = DayManifestV1(
        collector_id="tokyo01",
        utc_date=date(2026, 8, 10),
        sealed_at=datetime(2026, 8, 11, tzinfo=UTC),
        chunks=(manifest.as_ref(),),
    )
    files = {
        f"ready/{relative}": data,
        "ready/date=2026-08-10/writer=depth/chunk-test.manifest.json": (
            canonical_json_bytes(manifest)
        ),
        "ready/day-manifests/date=2026-08-10/SEALED.json": canonical_json_bytes(day),
    }
    return MemoryRemote(files), manifest
