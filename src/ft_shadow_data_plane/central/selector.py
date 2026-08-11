from __future__ import annotations

import gzip
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import orjson

from ft_shadow_data_plane.contracts.models import SYMBOL_PATTERN, UniverseDecisionV1
from ft_shadow_data_plane.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    sha256_bytes,
)


@dataclass(frozen=True, slots=True)
class RollingPolicy:
    liquidity_window_days: int = 7
    candidate_minimum_dwell_hours: int = 48
    core_minimum_dwell_days: int = 14
    candidate_daily_replacements: int = 2
    core_weekly_replacements: int = 5
    core_minimum_age_days: int = 30
    core_entry_rank: int = 45
    core_retain_rank: int = 55
    boundary_retain_rank: int = 10


@dataclass(frozen=True, slots=True)
class DiscoverySnapshot:
    observed_at: datetime
    exchange_info: bytes
    exchange_info_confirmation: bytes
    market_tickers: bytes

    @property
    def source_hashes(self) -> tuple[str, ...]:
        return (
            sha256_bytes(self.exchange_info),
            sha256_bytes(self.exchange_info_confirmation),
            sha256_bytes(self.market_tickers),
        )


@dataclass(frozen=True, slots=True)
class SelectionResult:
    core: tuple[str, ...]
    boundary: tuple[str, ...]
    probe: tuple[str, ...]
    inactive: tuple[str, ...]
    source_hashes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _MarketRow:
    symbol: str
    onboard_time_ms: int
    age_days: int
    mean_quote_volume: Decimal
    sample_count: int


def select_rolling_universe(
    active: UniverseDecisionV1,
    snapshots: tuple[DiscoverySnapshot, ...],
    *,
    effective_at: datetime,
    member_since: dict[str, datetime],
    core_since: dict[str, datetime],
    policy: RollingPolicy,
) -> SelectionResult:
    if not snapshots:
        raise ValueError("at least one discovery snapshot is required")
    latest = snapshots[-1]
    first_info = _symbol_info(latest.exchange_info)
    confirmed_info = _symbol_info(latest.exchange_info_confirmation)
    ticker_history = [_tickers(item.market_tickers) for item in snapshots]
    effective_ms = int(effective_at.timestamp() * 1000)

    eligible: dict[str, dict[str, Any]] = {}
    inactive: list[str] = []
    for symbol, raw in first_info.items():
        confirmation = confirmed_info.get(symbol)
        if confirmation is None:
            continue
        if _eligibility_reason(raw, symbol) is None and _eligibility_reason(
            confirmation, symbol
        ) is None:
            eligible[symbol] = confirmation
    for symbol in active.members:
        if symbol not in eligible:
            inactive.append(symbol)

    rows: list[_MarketRow] = []
    for symbol, raw in eligible.items():
        volumes = [value[symbol] for value in ticker_history if symbol in value]
        if not volumes:
            continue
        onboard_ms = _positive_int(raw.get("onboardDate"), f"{symbol}.onboardDate")
        if onboard_ms > effective_ms:
            continue
        rows.append(
            _MarketRow(
                symbol=symbol,
                onboard_time_ms=onboard_ms,
                age_days=(effective_ms - onboard_ms) // 86_400_000,
                mean_quote_volume=sum(volumes, Decimal()) / len(volumes),
                sample_count=len(volumes),
            )
        )
    liquidity = sorted(rows, key=lambda item: (-item.mean_quote_volume, item.symbol))
    rank = {item.symbol: index for index, item in enumerate(liquidity, start=1)}
    core = list(active.core)
    forced_core = [symbol for symbol in core if symbol in inactive]
    replacement_pool = [
        item.symbol
        for item in liquidity
        if item.symbol not in core
        and item.age_days >= policy.core_minimum_age_days
        and item.sample_count == len(snapshots)
    ]
    for symbol in forced_core:
        replacement = _take_first(replacement_pool, forbidden=set(core))
        if replacement is None:
            break
        core[core.index(symbol)] = replacement

    is_weekly = effective_at.weekday() == 0 and len(snapshots) >= policy.liquidity_window_days
    if is_weekly:
        changes = len(set(active.core) - set(core))
        promotable = [
            symbol
            for symbol in replacement_pool
            if symbol not in core
            if rank.get(symbol, 10**9) <= policy.core_entry_rank
        ]
        while promotable and changes < policy.core_weekly_replacements:
            challenger = promotable.pop(0)
            removable = [
                symbol
                for symbol in core
                if rank.get(symbol, 10**9) > policy.core_retain_rank
                if _dwell_complete(
                    core_since.get(symbol, active.effective_at),
                    effective_at,
                    timedelta(days=policy.core_minimum_dwell_days),
                )
            ]
            if not removable:
                break
            incumbent = max(removable, key=lambda symbol: rank.get(symbol, 10**9))
            if rank.get(challenger, 10**9) >= rank.get(incumbent, 10**9):
                break
            core[core.index(incumbent)] = challenger
            changes += 1

    core_set = set(core)
    newest = sorted(
        (item for item in rows if item.symbol not in core_set),
        key=lambda item: (-item.onboard_time_ms, -item.mean_quote_volume, item.symbol),
    )
    probe_preferred = [item.symbol for item in newest]
    probe = _reconcile_bucket(
        active.probe,
        probe_preferred,
        forbidden=core_set,
        inactive=set(inactive),
        member_since=member_since,
        effective_at=effective_at,
        minimum_dwell=timedelta(hours=policy.candidate_minimum_dwell_hours),
        normal_replacement_limit=1,
    )

    probe_set = set(probe)
    boundary_ranked = [
        item.symbol
        for item in liquidity
        if item.symbol not in core_set and item.symbol not in probe_set
    ]
    protected_boundary = {
        symbol
        for symbol in active.boundary
        if symbol not in inactive
        and symbol not in core_set
        and symbol not in probe_set
        and rank.get(symbol, 10**9) <= policy.boundary_retain_rank
    }
    boundary_preferred = [*sorted(protected_boundary), *boundary_ranked]
    boundary = _reconcile_bucket(
        active.boundary,
        boundary_preferred,
        forbidden=core_set | probe_set,
        inactive=set(inactive),
        member_since=member_since,
        effective_at=effective_at,
        minimum_dwell=timedelta(hours=policy.candidate_minimum_dwell_hours),
        normal_replacement_limit=max(1, policy.candidate_daily_replacements - 1),
    )

    source_hashes = tuple(value for item in snapshots for value in item.source_hashes)
    return SelectionResult(
        core=tuple(sorted(core)),
        boundary=tuple(sorted(boundary)),
        probe=tuple(sorted(probe)),
        inactive=tuple(sorted(inactive)),
        source_hashes=source_hashes,
    )


def write_formal_bundle(
    decision: UniverseDecisionV1,
    output_dir: Path,
    *,
    snapshot: DiscoverySnapshot,
) -> None:
    atomic_write_bytes(output_dir / "decision.json", canonical_json_bytes(decision), mode=0o644)
    atomic_write_bytes(
        output_dir / "formal-60.members.txt",
        ("\n".join(decision.members) + "\n").encode("ascii"),
        mode=0o644,
    )
    for name, content in (
        ("exchange-info.json.gz", snapshot.exchange_info),
        ("exchange-info-confirmation.json.gz", snapshot.exchange_info_confirmation),
        ("market-tickers.json.gz", snapshot.market_tickers),
    ):
        atomic_write_bytes(
            output_dir / "sources" / name,
            gzip.compress(content, mtime=0),
            mode=0o644,
        )


def _reconcile_bucket(
    current: tuple[str, ...],
    preferred: list[str],
    *,
    forbidden: set[str],
    inactive: set[str],
    member_since: dict[str, datetime],
    effective_at: datetime,
    minimum_dwell: timedelta,
    normal_replacement_limit: int,
) -> tuple[str, ...]:
    preferred = list(dict.fromkeys(symbol for symbol in preferred if symbol not in forbidden))
    result = [symbol for symbol in current if symbol not in forbidden and symbol not in inactive]
    forced_vacancies = 5 - len(result)
    for symbol in preferred:
        if len(result) >= 5:
            break
        if symbol not in result:
            result.append(symbol)
    normal_changes = 0
    for symbol in preferred:
        if symbol in result or normal_changes >= normal_replacement_limit:
            continue
        removable = [
            incumbent
            for incumbent in result
            if incumbent not in preferred[:5]
            and _dwell_complete(
                member_since.get(incumbent, effective_at), effective_at, minimum_dwell
            )
        ]
        if not removable:
            continue
        result.remove(removable[-1])
        result.append(symbol)
        normal_changes += 1
    if len(result) != 5:
        raise ValueError("not enough eligible instruments to fill candidate role")
    if forced_vacancies == 0 and len(set(current) - set(result)) > normal_replacement_limit:
        raise ValueError("candidate replacement limit exceeded")
    return tuple(sorted(result))


def _take_first(values: list[str], *, forbidden: set[str]) -> str | None:
    return next((value for value in values if value not in forbidden), None)


def _dwell_complete(joined: datetime, effective: datetime, required: timedelta) -> bool:
    return joined <= effective - required


def _symbol_info(raw: bytes) -> dict[str, dict[str, Any]]:
    payload = orjson.loads(raw)
    if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), list):
        raise ValueError("exchangeInfo must contain a symbols array")
    result: dict[str, dict[str, Any]] = {}
    for value in payload["symbols"]:
        if not isinstance(value, dict) or not isinstance(value.get("symbol"), str):
            raise ValueError("exchangeInfo contains an invalid symbol row")
        symbol = str(value["symbol"])
        if symbol in result:
            raise ValueError(f"exchangeInfo contains duplicate symbol: {symbol}")
        result[symbol] = value
    return result


def _tickers(raw: bytes) -> dict[str, Decimal]:
    payload = orjson.loads(raw)
    if not isinstance(payload, list):
        raise ValueError("market tickers must be an array")
    result: dict[str, Decimal] = {}
    for value in payload:
        if not isinstance(value, dict) or not isinstance(value.get("symbol"), str):
            raise ValueError("market tickers contains an invalid row")
        symbol = str(value["symbol"])
        try:
            volume = Decimal(str(value["quoteVolume"]))
        except (InvalidOperation, KeyError) as exc:
            raise ValueError(f"invalid quote volume for {symbol}") from exc
        if not volume.is_finite() or volume < 0:
            raise ValueError(f"invalid quote volume for {symbol}")
        result[symbol] = volume
    return result


def _eligibility_reason(raw: dict[str, Any], symbol: str) -> str | None:
    if raw.get("status") != "TRADING":
        return "not_trading"
    if raw.get("contractType") != "PERPETUAL":
        return "not_perpetual"
    if raw.get("quoteAsset") != "USDT" or raw.get("marginAsset") != "USDT":
        return "not_usdt"
    if not SYMBOL_PATTERN.fullmatch(symbol):
        return "invalid_symbol"
    return None


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError(f"{label} must be a positive integer")
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return parsed
