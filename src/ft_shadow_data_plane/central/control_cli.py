from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

from ft_shadow_data_plane.contracts.models import ControlReason, UniverseControlV1
from ft_shadow_data_plane.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    universe_hash,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a versioned edge universe control")
    parser.add_argument("--generation", type=int, required=True)
    parser.add_argument("--effective-at", type=_utc_datetime, required=True)
    parser.add_argument(
        "--reason",
        type=ControlReason,
        choices=(ControlReason.DAILY, ControlReason.NEW_LISTING_PROBE),
        required=True,
    )
    parser.add_argument("--members", required=True, help="comma-separated Binance symbols")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    members = tuple(
        sorted(set(value.strip().upper() for value in args.members.split(",") if value))
    )
    control = UniverseControlV1(
        generation=args.generation,
        created_at=datetime.now(UTC),
        effective_at=args.effective_at,
        reason=args.reason,
        members=members,
        universe_hash=universe_hash(members),
    )
    atomic_write_bytes(args.output, canonical_json_bytes(control))


def _utc_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise argparse.ArgumentTypeError("timestamp must include UTC offset")
    return parsed


if __name__ == "__main__":
    main()
