from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from ft_shadow_data_plane.contracts.models import ControlReason
from ft_shadow_data_plane.edge.service import EdgeService


class OnlineSources:
    running = True

    def __init__(self) -> None:
        self.stop_calls = 0
        self.start_calls: list[tuple[str, ...]] = []

    async def stop(self) -> None:
        self.stop_calls += 1

    async def start(self, members: tuple[str, ...]) -> None:
        self.start_calls.append(members)

    async def wait_ready(self) -> None:
        return None


class BoundaryGaps:
    def __init__(self) -> None:
        self.open_calls = 0
        self.close_calls = 0
        self.rollovers: list[datetime] = []
        self.universe_hashes: list[str] = []

    async def open(self, *args: object, **kwargs: object) -> str:
        self.open_calls += 1
        return "gap-boundary"

    async def close(self, *args: object, **kwargs: object) -> None:
        self.close_calls += 1

    async def rollover(self, boundary: datetime) -> None:
        self.rollovers.append(boundary)

    def set_universe_hash(self, value: str) -> None:
        self.universe_hashes.append(value)


class NoChangeUniverse:
    active = SimpleNamespace(members=("BTCUSDT",), universe_hash="a" * 64)

    def apply_due(
        self,
        now: datetime,
        *,
        reasons: frozenset[ControlReason],
    ) -> None:
        return None


class RecordingIngest:
    def __init__(self) -> None:
        self.rotations: list[str] = []

    async def rotate(self, *, universe_hash: str) -> None:
        self.rotations.append(universe_hash)


class RecordingDayIndex:
    def __init__(self) -> None:
        self.seals: list[tuple[date, datetime]] = []

    async def seal(self, utc_date: date, *, sealed_at: datetime) -> None:
        self.seals.append((utc_date, sealed_at))


@pytest.mark.asyncio
async def test_midnight_without_universe_change_keeps_sources_online() -> None:
    service = object.__new__(EdgeService)
    service._sources = OnlineSources()  # type: ignore[attr-defined]
    service._gaps = BoundaryGaps()  # type: ignore[attr-defined]
    service._universe_store = NoChangeUniverse()  # type: ignore[attr-defined]
    service._ingest = RecordingIngest()  # type: ignore[attr-defined]
    service._day_index = RecordingDayIndex()  # type: ignore[attr-defined]
    midnight = datetime(2026, 8, 11, tzinfo=UTC)
    reasons = frozenset({ControlReason.DAILY, ControlReason.CANARY_SCALE})

    await service._apply_midnight_boundary(
        midnight,
        reasons=reasons,
        planned_transition=False,
    )

    assert service._sources.stop_calls == 0  # type: ignore[attr-defined]
    assert service._sources.start_calls == []  # type: ignore[attr-defined]
    assert service._gaps.open_calls == 0  # type: ignore[attr-defined]
    assert service._gaps.close_calls == 0  # type: ignore[attr-defined]
    assert service._gaps.rollovers == [midnight]  # type: ignore[attr-defined]
    assert service._ingest.rotations == ["a" * 64]  # type: ignore[attr-defined]
    assert service._day_index.seals == [  # type: ignore[attr-defined]
        (date(2026, 8, 10), midnight)
    ]
