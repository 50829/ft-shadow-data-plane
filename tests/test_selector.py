from __future__ import annotations

from datetime import UTC, datetime, timedelta

import orjson

from ft_shadow_data_plane.central.selector import (
    DiscoverySnapshot,
    RollingPolicy,
    select_rolling_universe,
)
from ft_shadow_data_plane.contracts.models import UniverseDecisionReason, UniverseDecisionV1
from ft_shadow_data_plane.contracts.serde import universe_hash


def test_monday_core_rotation_uses_seven_days_and_hysteresis() -> None:
    effective = datetime(2026, 8, 17, tzinfo=UTC)  # Monday
    core = tuple(sorted((*_symbols(1, 50), "S069USDT")))
    boundary = _symbols(50, 55)
    probe = _symbols(55, 60)
    active = _decision(core, boundary, probe, effective - timedelta(days=30))
    snapshots = tuple(
        _snapshot(effective - timedelta(days=7 - day)) for day in range(7)
    )

    result = select_rolling_universe(
        active,
        snapshots,
        effective_at=effective,
        member_since={symbol: effective - timedelta(days=30) for symbol in active.members},
        core_since={symbol: effective - timedelta(days=30) for symbol in active.core},
        policy=RollingPolicy(),
    )

    assert "S000USDT" in result.core
    assert "S069USDT" not in result.core
    assert len(result.core) == 50
    assert len(set((*result.core, *result.boundary, *result.probe))) == 60


def test_daily_candidates_change_at_most_one_symbol_per_role() -> None:
    effective = datetime(2026, 8, 18, tzinfo=UTC)
    active = _decision(
        _symbols(0, 50),
        _symbols(60, 65),
        _symbols(50, 55),
        effective - timedelta(days=30),
    )
    result = select_rolling_universe(
        active,
        (_snapshot(effective - timedelta(hours=1)),),
        effective_at=effective,
        member_since={symbol: effective - timedelta(days=3) for symbol in active.members},
        core_since={symbol: effective - timedelta(days=30) for symbol in active.core},
        policy=RollingPolicy(),
    )

    assert result.core == active.core
    assert len(set(active.boundary) - set(result.boundary)) <= 1
    assert len(set(active.probe) - set(result.probe)) <= 1
    assert len(set((*result.core, *result.boundary, *result.probe))) == 60


def test_forced_core_replacement_is_not_promoted_twice_on_monday() -> None:
    effective = datetime(2026, 8, 17, tzinfo=UTC)  # Monday
    core = tuple(sorted((*_symbols(2, 50), "S068USDT", "S069USDT")))
    active = _decision(
        core,
        _symbols(50, 55),
        _symbols(55, 60),
        effective - timedelta(days=30),
    )
    snapshots = tuple(
        _snapshot(effective - timedelta(days=7 - day), inactive="S069USDT")
        for day in range(7)
    )

    result = select_rolling_universe(
        active,
        snapshots,
        effective_at=effective,
        member_since={symbol: effective - timedelta(days=30) for symbol in active.members},
        core_since={symbol: effective - timedelta(days=30) for symbol in active.core},
        policy=RollingPolicy(),
    )

    assert "S069USDT" not in result.core
    assert len(result.core) == len(set(result.core)) == 50
    assert len(set((*result.core, *result.boundary, *result.probe))) == 60


def _symbols(start: int, stop: int) -> tuple[str, ...]:
    return tuple(f"S{index:03}USDT" for index in range(start, stop))


def _decision(
    core: tuple[str, ...],
    boundary: tuple[str, ...],
    probe: tuple[str, ...],
    effective_at: datetime,
) -> UniverseDecisionV1:
    core = tuple(sorted(core))
    boundary = tuple(sorted(boundary))
    probe = tuple(sorted(probe))
    return UniverseDecisionV1(
        generation=1,
        created_at=effective_at,
        effective_at=effective_at,
        reason=UniverseDecisionReason.FORMAL_BOOTSTRAP,
        core=core,
        boundary=boundary,
        probe=probe,
        universe_hash=universe_hash(core, boundary, probe),
    )


def _snapshot(observed_at: datetime, *, inactive: str | None = None) -> DiscoverySnapshot:
    symbols = [
        {
            "symbol": symbol,
            "contractType": "PERPETUAL",
            "status": "SETTLING" if symbol == inactive else "TRADING",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "onboardDate": int(
                (observed_at - timedelta(days=100 - index)).timestamp() * 1000
            ),
        }
        for index, symbol in enumerate(_symbols(0, 70))
    ]
    exchange_info = orjson.dumps({"symbols": symbols})
    tickers = orjson.dumps(
        [
            {"symbol": symbol, "quoteVolume": str(100_000 - index * 1_000)}
            for index, symbol in enumerate(_symbols(0, 70))
        ]
    )
    return DiscoverySnapshot(observed_at, exchange_info, exchange_info, tickers)
