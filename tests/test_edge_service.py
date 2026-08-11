from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from ft_shadow_data_plane.edge.service import EdgeService


class OnlineSources:
    running = True

    def __init__(self) -> None:
        self.updates: list[tuple[str, ...]] = []

    async def update_instruments(self, members: tuple[str, ...]) -> None:
        self.updates.append(members)


class BoundaryGaps:
    def __init__(self) -> None:
        self.opened_symbols: list[tuple[str, ...]] = []
        self.closed_symbols: list[tuple[str, ...]] = []
        self.rollovers: list[datetime] = []
        self.universe_hashes: list[str] = []

    async def open(self, *args: object, **kwargs: object) -> str:
        self.opened_symbols.append(kwargs["exchange_symbols"])  # type: ignore[arg-type]
        return "gap-boundary"

    async def close(self, *args: object, **kwargs: object) -> None:
        self.closed_symbols.append(kwargs["exchange_symbols"])  # type: ignore[arg-type]

    async def rollover(self, boundary: datetime) -> None:
        self.rollovers.append(boundary)

    def set_universe_hash(self, value: str) -> None:
        self.universe_hashes.append(value)


class Universe:
    def __init__(self, previous: object, decision: object | None) -> None:
        self.active = previous
        self._decision = decision

    def apply_due(self, now: datetime) -> object | None:
        if self._decision is not None:
            self.active = self._decision
        return self._decision


class RecordingIngest:
    def __init__(self) -> None:
        self.rotations: list[str] = []

    async def rotate(self, *, universe_hash: str) -> None:
        self.rotations.append(universe_hash)


class RecordingDayIndex:
    def __init__(self) -> None:
        self.seals: list[date] = []

    async def seal(self, utc_date: date, *, sealed_at: datetime) -> None:
        self.seals.append(utc_date)


@pytest.mark.asyncio
async def test_midnight_without_change_keeps_all_sources_online() -> None:
    previous = SimpleNamespace(members=_members(), universe_hash="a" * 64)
    service = _service(previous, None)
    midnight = datetime(2026, 8, 11, tzinfo=UTC)

    await service._apply_midnight_boundary(midnight)

    assert service._sources.updates == []  # type: ignore[attr-defined]
    assert service._gaps.opened_symbols == []  # type: ignore[attr-defined]
    assert service._ingest.rotations == ["a" * 64]  # type: ignore[attr-defined]
    assert service._day_index.seals == [date(2026, 8, 10)]  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_one_symbol_change_only_marks_changed_symbols() -> None:
    previous_members = _members()
    next_members = tuple(sorted((*previous_members[:-1], "NEWUSDT")))
    previous = SimpleNamespace(members=previous_members, universe_hash="a" * 64)
    decision = SimpleNamespace(members=next_members, universe_hash="b" * 64)
    service = _service(previous, decision)

    await service._apply_midnight_boundary(datetime(2026, 8, 11, tzinfo=UTC))

    assert service._sources.updates == [next_members]  # type: ignore[attr-defined]
    assert service._gaps.opened_symbols == [("NEWUSDT", previous_members[-1])]  # type: ignore[attr-defined]
    assert service._gaps.closed_symbols == [("NEWUSDT", previous_members[-1])]  # type: ignore[attr-defined]
    assert service._ingest.rotations == ["b" * 64]  # type: ignore[attr-defined]


def _service(previous: object, decision: object | None) -> Any:
    service: Any = object.__new__(EdgeService)
    service._sources = OnlineSources()
    service._gaps = BoundaryGaps()
    service._universe_store = Universe(previous, decision)
    service._ingest = RecordingIngest()
    service._day_index = RecordingDayIndex()
    return service


def _members() -> tuple[str, ...]:
    return tuple(f"S{index:03}USDT" for index in range(60))
