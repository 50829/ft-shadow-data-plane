from __future__ import annotations

import argparse
import gzip
from datetime import UTC, datetime
from pathlib import Path

from ft_shadow_data_plane.central.selector import DiscoverySnapshot, write_formal_bundle
from ft_shadow_data_plane.contracts.models import UniverseDecisionReason, UniverseDecisionV1
from ft_shadow_data_plane.contracts.serde import universe_hash


def main() -> None:
    parser = argparse.ArgumentParser(description="Write a formal 60-instrument universe bundle")
    parser.add_argument("--exchange-info", type=Path, required=True)
    parser.add_argument("--exchange-info-confirmation", type=Path, required=True)
    parser.add_argument("--market-tickers", type=Path, required=True)
    parser.add_argument("--core", type=Path, required=True)
    parser.add_argument("--boundary", type=Path, required=True)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    now = datetime.now(UTC)
    snapshot = DiscoverySnapshot(
        observed_at=now,
        exchange_info=_read(args.exchange_info),
        exchange_info_confirmation=_read(args.exchange_info_confirmation),
        market_tickers=_read(args.market_tickers),
    )
    core = _members(args.core)
    boundary = _members(args.boundary)
    probe = _members(args.probe)
    decision = UniverseDecisionV1(
        generation=1,
        created_at=now,
        effective_at=now,
        reason=UniverseDecisionReason.FORMAL_BOOTSTRAP,
        core=core,
        boundary=boundary,
        probe=probe,
        source_hashes=snapshot.source_hashes,
        universe_hash=universe_hash(core, boundary, probe),
    )
    write_formal_bundle(decision, args.output_dir, snapshot=snapshot)


def _read(path: Path) -> bytes:
    content = path.read_bytes()
    return gzip.decompress(content) if path.suffix == ".gz" else content


def _members(path: Path) -> tuple[str, ...]:
    return tuple(sorted(value.strip().upper() for value in path.read_text().splitlines() if value))


if __name__ == "__main__":
    main()
