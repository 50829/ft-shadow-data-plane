from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

from ft_shadow_data_plane.edge.config import load_edge_config

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INSTALL_CAMPUS = PROJECT_ROOT / "deploy" / "campus-107" / "install.sh"
SUBMIT_DAY = PROJECT_ROOT / "deploy" / "campus-107" / "submit-day.sh"


def test_vultr_config_is_formal_sixty_and_memory_bounded() -> None:
    config = load_edge_config(PROJECT_ROOT / "deploy/vultr/edge.yaml.example")
    compose = (PROJECT_ROOT / "deploy/vultr/compose.yaml").read_text(encoding="ascii")
    service = (
        PROJECT_ROOT / "deploy/vultr/systemd/ft-shadow-data-plane.service"
    ).read_text(encoding="ascii")

    role_sizes = (
        len(config.universe.core),
        len(config.universe.boundary),
        len(config.universe.probe),
    )
    assert role_sizes == (
        50,
        5,
        5,
    )
    assert len(config.universe.members) == 60
    assert config.public_connection_shards == 2
    assert config.queue_max_bytes == 64 * 1024**2
    assert config.websocket_max_queue == 4
    assert config.writer_batch_bytes == 2 * 1024**2
    assert "mem_limit: 768m" in compose
    assert "cpus: 0.90" in compose
    assert "pids_limit: 256" in compose
    assert "--exit-code-from collector" in service


def test_campus_installer_uses_hash_named_release(tmp_path: Path) -> None:
    release = tmp_path / "downloaded.sif"
    release.write_bytes(b"immutable release")
    install_root = tmp_path / "persistent"
    fake_apptainer = tmp_path / "apptainer"
    _write_fake_apptainer(fake_apptainer)

    result = subprocess.run(
        [str(INSTALL_CAMPUS), str(release)],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "FT_APPTAINER": str(fake_apptainer),
            "FT_CAMPUS_ROOT": str(install_root),
        },
    )

    assert result.returncode == 0, result.stderr
    digest = hashlib.sha256(release.read_bytes()).hexdigest()
    versioned_release = install_root / f"ft-shadow-data-plane-{digest}.sif"
    active_release = install_root / "ft-shadow-data-plane.sif"
    assert versioned_release.read_bytes() == release.read_bytes()
    assert active_release.is_symlink()
    assert active_release.resolve() == versioned_release
    assert (install_root / "ft-shadow-data-plane.sandbox").is_symlink()
    assert os.access(install_root / "pull-once.sh", os.X_OK)
    assert (install_root / "central.yaml").is_file()
    assert (install_root / "deploy/campus-107/processing.env").is_file()


def test_submit_day_builds_dependency_chain(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    sbatch_log = tmp_path / "sbatch.log"
    _write_fake_sbatch(fake_bin / "sbatch")
    processing_env = _write_processing_env(tmp_path, concurrency=8)
    symbols = tmp_path / "symbols.txt"
    symbols.write_text("BTCUSDT\nETHUSDT\n", encoding="ascii")

    result = subprocess.run(
        [str(SUBMIT_DAY), "2026-08-10", str(symbols)],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "FT_PROCESSING_ENV": str(processing_env),
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "SBATCH_LOG": str(sbatch_log),
        },
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "normalize_job=101",
        "l2_job=102",
        "finalize_job=103",
    ]
    calls = sbatch_log.read_text(encoding="ascii").splitlines()
    assert calls[0].endswith("/slurm/normalize.sbatch")
    assert "--dependency=afterok:101" in calls[1]
    assert "--array=0-1%8" in calls[1]
    assert calls[1].endswith("/slurm/l2-array.sbatch")
    assert "--dependency=afterok:102" in calls[2]
    assert calls[2].endswith("/slurm/finalize.sbatch")


def test_submit_day_rejects_duplicate_symbols(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    sbatch_log = tmp_path / "sbatch.log"
    _write_fake_sbatch(fake_bin / "sbatch")
    processing_env = _write_processing_env(tmp_path, concurrency=8)
    symbols = tmp_path / "symbols.txt"
    symbols.write_text("BTCUSDT\nBTCUSDT\n", encoding="ascii")

    result = subprocess.run(
        [str(SUBMIT_DAY), "2026-08-10", str(symbols)],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "FT_PROCESSING_ENV": str(processing_env),
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "SBATCH_LOG": str(sbatch_log),
        },
    )

    assert result.returncode == 1
    assert "symbols must be unique" in result.stderr
    assert not sbatch_log.exists()


def _write_processing_env(tmp_path: Path, *, concurrency: int) -> Path:
    path = tmp_path / "processing.env"
    path.write_text(
        "\n".join(
            (
                f"FT_APPTAINER={tmp_path / 'apptainer'}",
                f"FT_DATA_IMAGE={tmp_path / 'release.sandbox'}",
                f"FT_RAW_ROOT={tmp_path / 'raw'}",
                f"FT_DERIVED_ROOT={tmp_path / 'derived'}",
                "FT_COLLECTOR=tokyo01",
                f"FT_L2_CONCURRENCY={concurrency}",
                "",
            )
        ),
        encoding="ascii",
    )
    return path


def _write_fake_sbatch(path: Path) -> None:
    path.write_text(
        """#!/bin/sh
set -eu
printf '%s\\n' "$*" >> "$SBATCH_LOG"
case "$*" in
    *normalize.sbatch) echo '101;cluster' ;;
    *l2-array.sbatch) echo '102;cluster' ;;
    *finalize.sbatch) echo '103;cluster' ;;
    *) exit 1 ;;
esac
""",
        encoding="ascii",
    )
    path.chmod(0o755)


def _write_fake_apptainer(path: Path) -> None:
    path.write_text(
        """#!/bin/sh
set -eu
test "$1" = build
test "$2" = --sandbox
mkdir -p "$3"
""",
        encoding="ascii",
    )
    path.chmod(0o755)
