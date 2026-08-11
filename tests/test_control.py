from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import orjson
import pytest

from ft_shadow_data_plane.central.selector import DiscoverySnapshot
from ft_shadow_data_plane.edge.config import UniversePolicyConfig
from ft_shadow_data_plane.edge.universe import UniverseStore


def test_clean_store_starts_generation_one_with_all_sixty(tmp_path: Path) -> None:
    now = datetime(2026, 8, 11, 12, tzinfo=UTC)
    store = UniverseStore(tmp_path, _policy())

    active = store.initialize(now)

    assert active.generation == 1
    assert len(active.core) == 50
    assert len(active.boundary) == 5
    assert len(active.probe) == 5
    assert len(active.members) == 60
    assert (tmp_path / "control/universe/active.json").is_file()


def test_confirmed_inactive_candidate_is_planned_for_next_midnight(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 11, 23, 50, tzinfo=UTC)
    store = UniverseStore(tmp_path, _policy())
    active = store.initialize(now - timedelta(days=3))
    (tmp_path / "control/formal-start.json").write_text("{}", encoding="ascii")
    snapshot = _snapshot(now, inactive=active.boundary[0])

    decision = store.observe_and_plan(snapshot, now=now)

    assert decision is not None
    assert decision.effective_at == datetime(2026, 8, 12, tzinfo=UTC)
    assert active.boundary[0] not in decision.members
    assert store.has_due(decision.effective_at)
    assert store.apply_due(decision.effective_at) == decision


def test_paused_automation_still_rejects_inactive_formal_universe(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 11, 23, 50, tzinfo=UTC)
    policy = _policy().model_copy(update={"automation_enabled": False})
    store = UniverseStore(tmp_path, policy)
    active = store.initialize(now - timedelta(days=3))

    with pytest.raises(ValueError, match="refusing formal start"):
        store.observe_and_plan(_snapshot(now, inactive=active.core[0]), now=now)


def test_formal_bootstrap_binds_confirmed_source_hashes(tmp_path: Path) -> None:
    now = datetime(2026, 8, 11, 23, 50, tzinfo=UTC)
    policy = _policy().model_copy(update={"automation_enabled": False})
    store = UniverseStore(tmp_path, policy)
    store.initialize(now - timedelta(days=3))
    snapshot = _snapshot(now)

    assert store.observe_and_plan(snapshot, now=now) is None
    assert store.active.source_hashes == snapshot.source_hashes
    persisted = (tmp_path / "control/universe/active.json").read_bytes()
    assert b'"source_hashes"' in persisted


def _members(start: int, stop: int) -> tuple[str, ...]:
    return tuple(f"S{index:03}USDT" for index in range(start, stop))


def _policy() -> UniversePolicyConfig:
    return UniversePolicyConfig(
        experiment_id="formal-test-60",
        core=_members(0, 50),
        boundary=_members(50, 55),
        probe=_members(55, 60),
    )


def _snapshot(observed_at: datetime, *, inactive: str | None = None) -> DiscoverySnapshot:
    first = []
    confirmation = []
    tickers = []
    for index, symbol in enumerate(_members(0, 70)):
        row = {
            "symbol": symbol,
            "contractType": "PERPETUAL",
            "status": "SETTLING" if symbol == inactive else "TRADING",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "onboardDate": int((observed_at - timedelta(days=100)).timestamp() * 1000),
        }
        first.append(row)
        confirmation.append(dict(row))
        tickers.append({"symbol": symbol, "quoteVolume": str(100_000 - index)})
    return DiscoverySnapshot(
        observed_at,
        orjson.dumps({"symbols": first}),
        orjson.dumps({"symbols": confirmation}),
        orjson.dumps(tickers),
    )
