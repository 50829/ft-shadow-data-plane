from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from ft_shadow_data_plane.contracts.models import ControlReason, UniverseControlV1
from ft_shadow_data_plane.contracts.serde import universe_hash


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
