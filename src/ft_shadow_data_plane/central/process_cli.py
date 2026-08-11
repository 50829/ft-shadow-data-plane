from __future__ import annotations

import argparse
import logging
from datetime import UTC, date, datetime
from pathlib import Path

import orjson

from ft_shadow_data_plane.central.clock_quality import build_clock_quality
from ft_shadow_data_plane.central.d0 import build_d0_audit
from ft_shadow_data_plane.central.gap_ledger import build_transport_gap_ledger
from ft_shadow_data_plane.central.l2 import L2CheckpointV1, L2DayReconstructor
from ft_shadow_data_plane.central.normalize import DayNormalizer
from ft_shadow_data_plane.contracts.serde import atomic_write_bytes, canonical_json_bytes


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate and process one sealed UTC day")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("normalize", "l2", "d0-audit", "finalize"):
        command = subparsers.add_parser(name)
        command.add_argument("--raw-root", type=Path, required=True)
        command.add_argument("--derived-root", type=Path, required=True)
        command.add_argument("--collector", required=True)
        command.add_argument("--date", type=date.fromisoformat, required=True)
        if name == "l2":
            command.add_argument("--symbol", required=True)
        if name == "finalize":
            command.add_argument("--symbols", required=True, help="comma-separated symbols")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.command == "normalize":
        result = DayNormalizer(
            raw_root=args.raw_root,
            derived_root=args.derived_root,
            collector_id=args.collector,
            utc_date=args.date,
        ).run()
        build_transport_gap_ledger(
            raw_root=args.raw_root,
            derived_root=args.derived_root,
            collector_id=args.collector,
            utc_date=args.date,
        )
        build_clock_quality(
            derived_root=args.derived_root,
            collector_id=args.collector,
            utc_date=args.date,
        )
        logging.info("normalization complete %s", result)
    elif args.command == "l2":
        changes, intervals = L2DayReconstructor(
            derived_root=args.derived_root,
            collector_id=args.collector,
            utc_date=args.date,
            exchange_symbol=args.symbol.upper(),
        ).run()
        logging.info("L2 complete state_changes=%d valid_intervals=%d", changes, intervals)
    elif args.command == "d0-audit":
        path = build_d0_audit(
            derived_root=args.derived_root,
            collector_id=args.collector,
            utc_date=args.date,
        )
        logging.info("D0 audit complete path=%s", path)
    else:
        symbols = tuple(sorted(set(value.upper() for value in args.symbols.split(",") if value)))
        _finalize(
            args.derived_root,
            collector_id=args.collector,
            utc_date=args.date,
            symbols=symbols,
        )


def _finalize(
    derived_root: Path, *, collector_id: str, utc_date: date, symbols: tuple[str, ...]
) -> None:
    typed_root = (
        derived_root
        / "typed"
        / f"collector={collector_id}"
        / f"date={utc_date.isoformat()}"
    )
    if not (typed_root / "_NORMALIZED.json").exists():
        raise FileNotFoundError("day is not normalized")
    quality_root = (
        derived_root
        / "quality"
        / f"collector={collector_id}"
        / f"date={utc_date.isoformat()}"
    )
    missing = []
    empty = []
    for symbol in symbols:
        symbol_root = quality_root / f"symbol={symbol}"
        validity_path = symbol_root / "l2-validity.jsonl"
        checkpoint_path = symbol_root / "l2-checkpoint.json"
        if not validity_path.exists() or not checkpoint_path.exists():
            missing.append(symbol)
            continue
        if validity_path.stat().st_size == 0:
            empty.append(symbol)
            continue
        _validate_validity(validity_path, utc_date=utc_date)
        checkpoint = L2CheckpointV1.model_validate_json(checkpoint_path.read_bytes())
        if (
            checkpoint.collector_id != collector_id
            or checkpoint.utc_date != utc_date
            or checkpoint.exchange_symbol != symbol
        ):
            raise ValueError(f"L2 checkpoint identity mismatch: {checkpoint_path}")
    if missing:
        raise FileNotFoundError(f"missing L2 outputs: {','.join(missing)}")
    if empty:
        raise ValueError(f"empty L2 validity: {','.join(empty)}")
    atomic_write_bytes(
        quality_root / "_PROCESSED.json",
        canonical_json_bytes(
            {
                "schema_version": 1,
                "collector_id": collector_id,
                "utc_date": utc_date.isoformat(),
                "symbols": list(symbols),
            }
        ),
    )


def _validate_validity(path: Path, *, utc_date: date) -> None:
    day_start = int(
        datetime.combine(utc_date, datetime.min.time(), UTC).timestamp()
        * 1_000_000_000
    )
    day_end = day_start + 86_400 * 1_000_000_000
    previous_end: int | None = None
    with path.open("rb") as source:
        for line_number, line in enumerate(source, start=1):
            try:
                interval = orjson.loads(line)
                start = int(interval["valid_from_ns"])
                end = int(interval["valid_to_ns"])
            except (KeyError, TypeError, ValueError, orjson.JSONDecodeError) as exc:
                raise ValueError(
                    f"invalid L2 validity row {path}:{line_number}"
                ) from exc
            if (
                interval.get("schema_version") != 1
                or not isinstance(interval.get("connection_id"), str)
                or not interval["connection_id"]
                or not day_start <= start < end <= day_end
                or (previous_end is not None and start < previous_end)
            ):
                raise ValueError(f"invalid L2 validity ordering {path}:{line_number}")
            previous_end = end


if __name__ == "__main__":
    main()
