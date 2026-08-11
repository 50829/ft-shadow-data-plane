from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from universe_fixtures import formal_roles, liquidity_snapshot, symbols

from ft_shadow_data_plane.central.selector import (
    RollingPolicy,
    select_bootstrap_universe,
    select_rolling_universe,
)
from ft_shadow_data_plane.contracts.models import UniverseDecisionReason, UniverseDecisionV1
from ft_shadow_data_plane.contracts.serde import universe_hash


def test_bootstrap_selects_fifty_five_five_from_complete_evidence() -> None:
    observed = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)

    result = select_bootstrap_universe(
        liquidity_snapshot(observed), policy=RollingPolicy()
    )

    assert (result.core, result.boundary, result.probe) == formal_roles()
    assert len(set((*result.core, *result.boundary, *result.probe))) == 60
    assert len(result.source_hashes) == 5


def test_bootstrap_excludes_probe_without_fourteen_complete_days() -> None:
    observed = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)

    result = select_bootstrap_universe(
        liquidity_snapshot(observed, incomplete=frozenset({"S069USDT"})),
        policy=RollingPolicy(),
    )

    assert "S069USDT" not in result.probe
    assert "S000USDT" in result.probe


def test_bootstrap_fails_closed_when_trade_count_leaves_too_few_stable() -> None:
    observed = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)

    with pytest.raises(ValueError, match="stable candidates"):
        select_bootstrap_universe(
            liquidity_snapshot(observed, low_trades=frozenset(symbols(0, 11))),
            policy=RollingPolicy(),
        )


def test_monday_core_rotation_uses_robust_rank_and_hysteresis() -> None:
    effective = datetime(2026, 8, 17, tzinfo=UTC)
    active = _decision(
        tuple(sorted((*symbols(1, 50), "S064USDT"))),
        symbols(50, 55),
        symbols(65, 70),
        effective - timedelta(days=30),
    )

    result = select_rolling_universe(
        active,
        liquidity_snapshot(effective - timedelta(minutes=10)),
        effective_at=effective,
        member_since={symbol: effective - timedelta(days=30) for symbol in active.members},
        core_since={symbol: effective - timedelta(days=30) for symbol in active.core},
        policy=RollingPolicy(),
    )

    assert "S000USDT" in result.core
    assert "S064USDT" not in result.core


def test_boundary_hysteresis_uses_candidate_relative_top_ten() -> None:
    effective = datetime(2026, 8, 18, tzinfo=UTC)
    active = _decision(
        symbols(0, 50),
        symbols(56, 61),
        symbols(65, 70),
        effective - timedelta(days=30),
    )

    result = select_rolling_universe(
        active,
        liquidity_snapshot(effective - timedelta(minutes=10)),
        effective_at=effective,
        member_since={symbol: effective - timedelta(days=3) for symbol in active.members},
        core_since={symbol: effective - timedelta(days=30) for symbol in active.core},
        policy=RollingPolicy(),
    )

    assert set(symbols(56, 60)).issubset(result.boundary)
    assert "S060USDT" not in result.boundary
    assert len(set(active.boundary) - set(result.boundary)) == 1


def test_rolling_selection_fails_closed_when_stable_pool_cannot_fill_roles() -> None:
    effective = datetime(2026, 8, 18, tzinfo=UTC)
    core, boundary, probe = formal_roles()
    active = _decision(core, boundary, probe, effective - timedelta(days=30))

    result = select_rolling_universe(
        active,
        liquidity_snapshot(
            effective - timedelta(minutes=10),
            inactive="S000USDT",
            low_trades=frozenset(symbols(1, 12)),
        ),
        effective_at=effective,
        member_since={symbol: effective - timedelta(days=30) for symbol in active.members},
        core_since={symbol: effective - timedelta(days=30) for symbol in active.core},
        policy=RollingPolicy(),
    )

    assert (result.core, result.boundary, result.probe) == (
        active.core,
        active.boundary,
        active.probe,
    )
    assert result.inactive == ("S000USDT",)
    assert result.stable_pool_count == 53


def _decision(
    core: tuple[str, ...],
    boundary: tuple[str, ...],
    probe: tuple[str, ...],
    effective_at: datetime,
) -> UniverseDecisionV1:
    return UniverseDecisionV1(
        generation=1,
        created_at=effective_at,
        effective_at=effective_at,
        reason=UniverseDecisionReason.FORMAL_BOOTSTRAP,
        core=tuple(sorted(core)),
        boundary=tuple(sorted(boundary)),
        probe=tuple(sorted(probe)),
        universe_hash=universe_hash(core, boundary, probe),
    )
