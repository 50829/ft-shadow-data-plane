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
        choices=(
            ControlReason.DAILY,
            ControlReason.CANARY_SCALE,
            ControlReason.NEW_LISTING_PROBE,
        ),
        required=True,
    )
    members_group = parser.add_mutually_exclusive_group(required=True)
    members_group.add_argument("--members", help="comma-separated Binance symbols")
    members_group.add_argument(
        "--members-file", type=Path, help="one Binance symbol per line"
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw_members = (
        args.members.split(",")
        if args.members is not None
        else args.members_file.read_text(encoding="ascii").splitlines()
    )
    members = tuple(sorted(value.strip().upper() for value in raw_members if value.strip()))
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
