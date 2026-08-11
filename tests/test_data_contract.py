from __future__ import annotations

import orjson

from ft_shadow_data_plane.contracts.data_contract import (
    data_contract_hash_v1,
    data_contract_v1,
)


def test_data_contract_tracks_d0_and_open_interest_configuration() -> None:
    default = orjson.loads(data_contract_v1())
    d0 = orjson.loads(data_contract_v1(d0_enabled=True))

    assert default["d0_enabled"] is False
    assert default["gap_schema"] == 2
    assert {"daily_klines", "liquidity_depth"} <= set(default["streams"])
    assert {"trade", "rpi_depth", "rpi_depth_snapshot"}.isdisjoint(default["streams"])
    assert {"trade", "rpi_depth", "rpi_depth_snapshot"} <= set(d0["streams"])
    assert data_contract_hash_v1(d0_enabled=True) != data_contract_hash_v1()
    assert data_contract_hash_v1(open_interest_interval_seconds=60) != data_contract_hash_v1()
