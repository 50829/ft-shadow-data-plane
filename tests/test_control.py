from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from universe_fixtures import formal_roles, liquidity_snapshot

from ft_shadow_data_plane.edge.config import UniversePolicyConfig
from ft_shadow_data_plane.edge.universe import UniverseStore

EVIDENCE_HASH = "d" * 64


def test_clean_store_starts_generation_one_with_research_hash(tmp_path: Path) -> None:
    now = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)
    store = UniverseStore(tmp_path, _policy())

    active = store.initialize(now)

    assert active.generation == 1
    assert (active.core, active.boundary, active.probe) == formal_roles()
    assert active.source_hashes == (EVIDENCE_HASH,)


def test_formal_bootstrap_validates_members_and_binds_live_sources(tmp_path: Path) -> None:
    now = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)
    store = UniverseStore(tmp_path, _policy())
    store.initialize(now)
    snapshot = liquidity_snapshot(now)

    assert store.observe_and_plan(snapshot, now=now) is None
    assert store.active.source_hashes == (EVIDENCE_HASH, *snapshot.source_hashes)
    assert (tmp_path / "control/universe/evaluations").is_dir()


def test_formal_bootstrap_rejects_configured_members_that_fail_live_gates(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)
    core, _, _ = formal_roles()
    store = UniverseStore(tmp_path, _policy())
    store.initialize(now)

    with pytest.raises(ValueError, match="fail current formal liquidity gates"):
        store.observe_and_plan(
            liquidity_snapshot(now, low_trades=frozenset({core[0]})), now=now
        )

    assert core == store.active.core


def test_confirmed_inactive_candidate_is_planned_after_formal_start(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 18, 23, 50, tzinfo=UTC)
    store = UniverseStore(tmp_path, _policy())
    active = store.initialize(now - timedelta(days=3))
    (tmp_path / "control/formal-start.json").write_text("{}", encoding="ascii")

    decision = store.observe_and_plan(
        liquidity_snapshot(now, inactive=active.boundary[0]), now=now
    )

    assert decision is not None
    assert active.boundary[0] not in decision.members
    assert decision.effective_at == datetime(2026, 8, 19, tzinfo=UTC)


def _policy() -> UniversePolicyConfig:
    core, boundary, probe = formal_roles()
    return UniversePolicyConfig(
        experiment_id="formal-test-60",
        bootstrap_evidence_sha256=EVIDENCE_HASH,
        core=core,
        boundary=boundary,
        probe=probe,
    )
