from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ft_shadow_data_plane.contracts.models import SYMBOL_PATTERN


class UniversePolicyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    experiment_id: str = Field(min_length=8, max_length=160)
    core: tuple[str, ...] = Field(min_length=50, max_length=50)
    boundary: tuple[str, ...] = Field(min_length=5, max_length=5)
    probe: tuple[str, ...] = Field(min_length=5, max_length=5)
    discovery_hour_utc: int = Field(default=23, ge=0, le=23)
    discovery_minute_utc: int = Field(default=50, ge=0, le=59)
    decision_cutoff_minute_utc: int = Field(default=55, ge=0, le=59)
    automation_enabled: bool = True
    liquidity_window_days: int = Field(default=7, ge=1, le=30)
    candidate_minimum_dwell_hours: int = Field(default=48, ge=1)
    core_minimum_dwell_days: int = Field(default=14, ge=1)
    candidate_daily_replacements: int = Field(default=2, ge=1, le=2)
    core_weekly_replacements: int = Field(default=5, ge=1, le=5)
    core_minimum_age_days: int = Field(default=30, ge=1)
    core_entry_rank: int = Field(default=45, ge=1, le=50)
    core_retain_rank: int = Field(default=55, ge=50)
    boundary_retain_rank: int = Field(default=10, ge=5)

    @field_validator("core", "boundary", "probe")
    @classmethod
    def validate_role(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted(value.upper() for value in values))
        if any(not SYMBOL_PATTERN.fullmatch(value) for value in normalized):
            raise ValueError("universe role contains an invalid symbol")
        if len(set(normalized)) != len(normalized):
            raise ValueError("universe role contains duplicates")
        return normalized

    @model_validator(mode="after")
    def validate_roles(self) -> UniversePolicyConfig:
        members = (*self.core, *self.boundary, *self.probe)
        if len(set(members)) != 60:
            raise ValueError("universe roles must contain 60 distinct symbols")
        if self.decision_cutoff_minute_utc <= self.discovery_minute_utc:
            raise ValueError("decision cutoff must follow discovery in the same UTC hour")
        return self

    @property
    def members(self) -> tuple[str, ...]:
        return tuple(sorted((*self.core, *self.boundary, *self.probe)))


class EdgeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    collector_id: str = Field(min_length=1, max_length=100)
    data_root: Path
    universe: UniversePolicyConfig
    public_ws_url: str
    market_ws_url: str
    rest_url: str
    public_connection_shards: int = Field(default=2, ge=1, le=4)
    connection_rotation_seconds: int = Field(default=82_800, ge=3_600, le=86_000)
    connection_overlap_seconds: int = Field(default=15, ge=1, le=120)
    websocket_receive_timeout_seconds: float = Field(default=30.0, ge=5, le=300)
    websocket_ping_interval_seconds: float = Field(default=20.0, ge=5, le=60)
    websocket_ping_timeout_seconds: float = Field(default=20.0, ge=5, le=60)
    websocket_max_queue: int = Field(default=4, ge=1, le=16)
    websocket_max_message_bytes: int = Field(default=2 * 1024**2, ge=1024**2)
    symbol_liveness_seconds: float = Field(default=120.0, ge=30, le=600)
    open_interest_interval_seconds: int = Field(default=30, ge=10, le=300)
    clock_sample_interval_seconds: int = Field(default=60, ge=10, le=300)
    snapshot_request_interval_seconds: float = Field(default=2.0, ge=0.5, le=10)
    queue_max_bytes: int = Field(default=64 * 1024**2, ge=16 * 1024**2)
    queue_warn_ratio: float = Field(default=0.70, gt=0, lt=1)
    queue_resume_ratio: float = Field(default=0.50, gt=0, lt=1)
    chunk_max_seconds: int = Field(default=60, ge=5, le=300)
    chunk_max_bytes: int = Field(default=256 * 1024**2, ge=1024**2)
    chunk_max_events: int = Field(default=1_000_000, ge=1_000)
    writer_batch_events: int = Field(default=2_000, ge=100)
    writer_batch_bytes: int = Field(default=2 * 1024**2, ge=64 * 1024)
    spool_max_bytes: int = Field(default=10 * 1024**3, ge=1024**3)
    minimum_free_bytes: int = Field(default=5 * 1024**3, ge=1024**3)
    storage_check_seconds: int = Field(default=5, ge=1, le=60)
    d0_enabled: bool = False
    log_level: str = "INFO"

    @model_validator(mode="after")
    def validate_ratios(self) -> EdgeConfig:
        if self.queue_resume_ratio >= self.queue_warn_ratio:
            raise ValueError("queue_resume_ratio must be below queue_warn_ratio")
        return self


def load_edge_config(path: Path) -> EdgeConfig:
    with path.open("rb") as source:
        raw = yaml.safe_load(source)
    return EdgeConfig.model_validate(raw)
