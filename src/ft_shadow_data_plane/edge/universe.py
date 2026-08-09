from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import orjson

from ft_shadow_data_plane.contracts.models import ControlReason, UniverseControlV1
from ft_shadow_data_plane.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    universe_hash,
)

logger = logging.getLogger(__name__)


class UniverseStore:
    def __init__(self, data_root: Path, bootstrap_instruments: tuple[str, ...]) -> None:
        self._active_path = data_root / "control" / "universe" / "active.json"
        self._membership_path = (
            data_root / "control" / "universe" / "membership-since.json"
        )
        self._inbox = data_root / "control" / "universe" / "inbox"
        self._bootstrap = bootstrap_instruments
        self._active: UniverseControlV1 | None = None
        self._membership_since: dict[str, datetime] = {}

    @property
    def active(self) -> UniverseControlV1:
        if self._active is None:
            raise RuntimeError("universe store has not been initialized")
        return self._active

    def initialize(self) -> UniverseControlV1:
        self._inbox.mkdir(parents=True, exist_ok=True)
        if self._active_path.exists():
            self._active = UniverseControlV1.model_validate_json(self._active_path.read_bytes())
            self._load_or_initialize_membership()
            return self._active
        now = datetime.now(UTC)
        self._active = UniverseControlV1(
            generation=1,
            created_at=now,
            effective_at=now,
            reason=ControlReason.BOOTSTRAP,
            members=self._bootstrap,
            universe_hash=universe_hash(self._bootstrap),
        )
        atomic_write_bytes(self._active_path, canonical_json_bytes(self._active))
        self._membership_since = {member: now for member in self._active.members}
        self._write_membership()
        return self._active

    def apply_due(
        self,
        now: datetime | None = None,
        *,
        reasons: frozenset[ControlReason] | None = None,
    ) -> UniverseControlV1 | None:
        now = now or datetime.now(UTC)
        candidates: list[tuple[Path, UniverseControlV1]] = []
        for path in sorted(self._inbox.glob("*.control.json")):
            try:
                control = UniverseControlV1.model_validate_json(path.read_bytes())
            except ValueError:
                logger.exception("ignoring invalid universe control path=%s", path)
                continue
            if (
                control.generation > self.active.generation
                and control.effective_at <= now
                and (reasons is None or control.reason in reasons)
            ):
                candidates.append((path, control))
        if not candidates:
            return None
        selected_path, selected = max(candidates, key=lambda item: item[1].generation)
        try:
            self._validate_transition(selected)
        except ValueError:
            logger.exception("rejecting unsafe universe transition path=%s", selected_path)
            selected_path.replace(selected_path.with_suffix(".rejected.json"))
            return None
        effective = selected.effective_at
        self._membership_since = {
            member: self._membership_since.get(member, effective) for member in selected.members
        }
        self._write_membership()
        atomic_write_bytes(self._active_path, canonical_json_bytes(selected))
        self._active = selected
        for path in self._inbox.glob("*.control.json"):
            try:
                control = UniverseControlV1.model_validate_json(path.read_bytes())
            except ValueError:
                continue
            if control.generation <= selected.generation:
                path.unlink(missing_ok=True)
        return selected

    def _validate_transition(self, selected: UniverseControlV1) -> None:
        current = set(self.active.members)
        proposed = set(selected.members)
        removed = current - proposed
        added = proposed - current
        if selected.reason is ControlReason.DAILY:
            if len(removed) > 5 or len(added) > 5:
                raise ValueError("daily control may replace at most five members")
            minimum_joined_at = selected.effective_at - timedelta(hours=48)
            too_young = [
                member
                for member in removed
                if self._membership_since.get(member, selected.effective_at) > minimum_joined_at
            ]
            if too_young:
                raise ValueError(f"daily control violates 48h dwell: {too_young}")
        elif selected.reason is ControlReason.NEW_LISTING_PROBE and removed:
            raise ValueError("new-listing probe control cannot remove existing members")

    def _load_or_initialize_membership(self) -> None:
        if self._membership_path.exists():
            raw = orjson.loads(self._membership_path.read_bytes())
            if not isinstance(raw, dict):
                raise ValueError("membership-since state must be an object")
            self._membership_since = {
                member: datetime.fromisoformat(str(raw[member]))
                for member in self.active.members
                if member in raw
            }
        effective = self.active.effective_at
        for member in self.active.members:
            self._membership_since.setdefault(member, effective)
        self._write_membership()

    def _write_membership(self) -> None:
        atomic_write_bytes(
            self._membership_path,
            canonical_json_bytes(
                {
                    member: joined_at.isoformat()
                    for member, joined_at in sorted(self._membership_since.items())
                }
            ),
        )
