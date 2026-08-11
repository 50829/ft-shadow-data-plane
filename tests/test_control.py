from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from ft_shadow_data_plane.contracts.models import ControlReason, UniverseControlV1
from ft_shadow_data_plane.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    universe_hash,
)
from ft_shadow_data_plane.edge.universe import UniverseStore


def test_daily_control_is_only_valid_at_utc_midnight() -> None:
    members = ("BTCUSDT", "ETHUSDT")
    created = datetime(2026, 8, 10, 12, tzinfo=UTC)
    valid = UniverseControlV1(
        generation=2,
        created_at=created,
        effective_at=datetime(2026, 8, 11, tzinfo=UTC),
        reason=ControlReason.DAILY,
        members=members,
        universe_hash=universe_hash(members),
    )
    assert valid.effective_at.hour == 0
    with pytest.raises(ValidationError, match="00:00 UTC"):
        UniverseControlV1(
            generation=2,
            created_at=created,
            effective_at=datetime(2026, 8, 11, 1, tzinfo=UTC),
            reason=ControlReason.DAILY,
            members=members,
            universe_hash=universe_hash(members),
        )
    with pytest.raises(ValidationError, match="00:00 UTC"):
        UniverseControlV1(
            generation=2,
            created_at=created,
            effective_at=datetime(2026, 8, 11, microsecond=1, tzinfo=UTC),
            reason=ControlReason.DAILY,
            members=members,
            universe_hash=universe_hash(members),
        )


def test_canary_control_scales_through_fixed_nested_stages(tmp_path: Path) -> None:
    created = datetime(2026, 8, 10, 12, tzinfo=UTC)
    effective = datetime(2026, 8, 11, tzinfo=UTC)
    store = UniverseStore(tmp_path, _members(20))
    store.initialize()

    stage_40 = _control(2, effective, _members(40), created=created)
    _write_control(tmp_path, stage_40)
    applied = store.apply_due(effective, reasons=frozenset({ControlReason.CANARY_SCALE}))
    assert applied is not None
    assert applied.members == _members(40)

    skipped_stage = _control(3, effective, _members(60), created=created)
    _write_control(tmp_path, skipped_stage)
    assert store.apply_due(
        effective, reasons=frozenset({ControlReason.CANARY_SCALE})
    ) is None
    assert store.active.members == _members(40)


def test_canary_control_cannot_remove_members(tmp_path: Path) -> None:
    created = datetime(2026, 8, 10, 12, tzinfo=UTC)
    effective = datetime(2026, 8, 11, tzinfo=UTC)
    initial = _members(20)
    store = UniverseStore(tmp_path, initial)
    store.initialize()
    proposed = tuple(sorted((*initial[1:], *tuple(f"N{index:02}USDT" for index in range(21)))))
    control = _control(2, effective, proposed, created=created)
    _write_control(tmp_path, control)

    assert store.apply_due(
        effective, reasons=frozenset({ControlReason.CANARY_SCALE})
    ) is None
    assert store.active.members == initial


def test_has_due_does_not_apply_or_remove_control(tmp_path: Path) -> None:
    effective = datetime(2026, 8, 11, tzinfo=UTC)
    store = UniverseStore(tmp_path, _members(20))
    initial = store.initialize()
    control = _control(
        2,
        effective,
        _members(40),
        created=effective - timedelta(hours=12),
    )
    _write_control(tmp_path, control)

    assert store.has_due(
        effective, reasons=frozenset({ControlReason.CANARY_SCALE})
    )
    assert store.active == initial
    assert (tmp_path / "control/universe/inbox/2.control.json").exists()


def test_final_canary_enters_steady_state_before_accepting_probes(tmp_path: Path) -> None:
    store = UniverseStore(tmp_path, _members(20))
    store.initialize()
    for generation, size, effective in (
        (2, 40, datetime(2026, 8, 11, tzinfo=UTC)),
        (3, 50, datetime(2026, 8, 12, tzinfo=UTC)),
        (4, 60, datetime(2026, 8, 13, tzinfo=UTC)),
    ):
        control = _control(
            generation,
            effective,
            _members(size),
            created=effective - timedelta(hours=12),
        )
        _write_control(tmp_path, control)
        assert store.apply_due(
            effective, reasons=frozenset({ControlReason.CANARY_SCALE})
        ) is not None

    steady_at = datetime(2026, 8, 16, tzinfo=UTC)
    steady = _control(
        5,
        steady_at,
        _members(55),
        created=steady_at - timedelta(hours=12),
        reason=ControlReason.DAILY,
    )
    _write_control(tmp_path, steady)
    assert store.apply_due(steady_at, reasons=frozenset({ControlReason.DAILY})) is not None

    probe_members = tuple(sorted((*_members(55), "NEWUSDT")))
    probe = _control(
        6,
        steady_at + timedelta(hours=1),
        probe_members,
        created=steady_at + timedelta(minutes=30),
        reason=ControlReason.NEW_LISTING_PROBE,
    )
    _write_control(tmp_path, probe)
    assert store.apply_due(
        steady_at + timedelta(hours=1),
        reasons=frozenset({ControlReason.NEW_LISTING_PROBE}),
    ) is not None
    assert store.active.members == probe_members


def _members(size: int) -> tuple[str, ...]:
    return tuple(f"S{index:02}USDT" for index in range(size))


def _control(
    generation: int,
    effective_at: datetime,
    members: tuple[str, ...],
    *,
    created: datetime,
    reason: ControlReason = ControlReason.CANARY_SCALE,
) -> UniverseControlV1:
    return UniverseControlV1(
        generation=generation,
        created_at=created,
        effective_at=effective_at,
        reason=reason,
        members=members,
        universe_hash=universe_hash(members),
    )


def _write_control(root: Path, control: UniverseControlV1) -> None:
    atomic_write_bytes(
        root / "control/universe/inbox" / f"{control.generation}.control.json",
        canonical_json_bytes(control),
    )
