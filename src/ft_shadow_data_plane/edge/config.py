from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ft_shadow_data_plane.contracts.models import SYMBOL_PATTERN


class EdgeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    collector_id: str = Field(min_length=1, max_length=100)
    data_root: Path
    bootstrap_instruments: tuple[str, ...] = Field(min_length=1, max_length=60)
    public_ws_url: str
    market_ws_url: str
    rest_url: str
    public_connection_shards: int = Field(default=4, ge=1, le=8)
    connection_rotation_seconds: int = Field(default=82_800, ge=3_600, le=86_000)
    connection_overlap_seconds: int = Field(default=15, ge=1, le=120)
    open_interest_interval_seconds: int = Field(default=30, ge=10, le=300)
    exchange_info_interval_seconds: int = Field(default=86_400, ge=3_600)
    clock_sample_interval_seconds: int = Field(default=60, ge=10, le=300)
    snapshot_request_interval_seconds: float = Field(default=2.0, ge=0.5, le=10)
    queue_max_bytes: int = Field(default=128 * 1024**2, ge=16 * 1024**2)
    queue_warn_ratio: float = Field(default=0.70, gt=0, lt=1)
    queue_resume_ratio: float = Field(default=0.50, gt=0, lt=1)
    chunk_max_seconds: int = Field(default=60, ge=5, le=300)
    chunk_max_bytes: int = Field(default=256 * 1024**2, ge=1024**2)
    chunk_max_events: int = Field(default=1_000_000, ge=1_000)
    writer_batch_events: int = Field(default=5_000, ge=100)
    writer_batch_bytes: int = Field(default=4 * 1024**2, ge=64 * 1024)
    spool_max_bytes: int = Field(default=10 * 1024**3, ge=1024**3)
    minimum_free_bytes: int = Field(default=5 * 1024**3, ge=1024**3)
    storage_check_seconds: int = Field(default=5, ge=1, le=60)
    control_poll_seconds: int = Field(default=30, ge=5, le=300)
    d0_enabled: bool = False
    log_level: str = "INFO"

    @field_validator("bootstrap_instruments")
    @classmethod
    def validate_instruments(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted(value.upper() for value in values))
        if any(not SYMBOL_PATTERN.fullmatch(value) for value in normalized):
            raise ValueError("bootstrap_instruments contains an invalid symbol")
        if len(normalized) != len(set(normalized)):
            raise ValueError("bootstrap_instruments contains duplicates")
        return normalized

    @model_validator(mode="after")
    def validate_ratios(self) -> EdgeConfig:
        if self.queue_resume_ratio >= self.queue_warn_ratio:
            raise ValueError("queue_resume_ratio must be below queue_warn_ratio")
        return self


def load_edge_config(path: Path) -> EdgeConfig:
    with path.open("rb") as source:
        raw = yaml.safe_load(source)
    return EdgeConfig.model_validate(raw)
