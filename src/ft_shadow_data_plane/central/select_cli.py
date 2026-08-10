from __future__ import annotations

import argparse
import gzip
import logging
from pathlib import Path

from ft_shadow_data_plane.central.selector import (
    BootstrapPolicy,
    select_bootstrap_universe,
    write_bootstrap_bundle,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build an auditable instrument universe")
    subparsers = parser.add_subparsers(dest="command", required=True)
    bootstrap = subparsers.add_parser("bootstrap")
    bootstrap.add_argument("--exchange-info", type=Path, required=True)
    bootstrap.add_argument("--market-tickers", type=Path, required=True)
    bootstrap.add_argument("--output-dir", type=Path, required=True)
    bootstrap.add_argument("--core-min-age-days", type=int, default=30)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    exchange_info_bytes = _read_payload(args.exchange_info)
    market_tickers_bytes = _read_payload(args.market_tickers)
    selection = select_bootstrap_universe(
        exchange_info_bytes,
        market_tickers_bytes,
        policy=BootstrapPolicy(core_min_age_days=args.core_min_age_days),
    )
    write_bootstrap_bundle(
        selection,
        args.output_dir,
        exchange_info_bytes=exchange_info_bytes,
        market_tickers_bytes=market_tickers_bytes,
    )
    logging.info(
        "bootstrap universe written output=%s stages=%s",
        args.output_dir,
        ",".join(str(size) for size in sorted(selection.stages)),
    )


def _read_payload(path: Path) -> bytes:
    content = path.read_bytes()
    return gzip.decompress(content) if path.suffix == ".gz" else content


if __name__ == "__main__":
    main()
