from __future__ import annotations

import argparse
import logging
from pathlib import Path

from ft_shadow_data_plane.central.config import load_central_config
from ft_shadow_data_plane.central.pull import CentralPuller, ParamikoRemoteStore, safe_remote_root


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull durable raw chunks from the edge collector")
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = load_central_config(args.config)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    with ParamikoRemoteStore(config) as remote:
        puller = CentralPuller(
            remote,
            remote_ready_root=safe_remote_root(config.remote_ready_root),
            remote_ack_root=safe_remote_root(config.remote_ack_root),
            local_raw_root=config.local_raw_root,
        )
        pulled, failures = puller.run()
    logging.info("pull complete new_chunks=%d failures=%d", pulled, failures)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
