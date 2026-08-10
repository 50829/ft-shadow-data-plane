from __future__ import annotations

import gzip
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import orjson

from ft_shadow_data_plane.contracts.models import CANARY_STAGE_SIZES, SYMBOL_PATTERN
from ft_shadow_data_plane.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    sha256_bytes,
    universe_hash,
)

EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
MARKET_TICKERS_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
@dataclass(frozen=True, slots=True)
class BootstrapPolicy:
    quote_asset: str = "USDT"
    core_size: int = 50
    boundary_size: int = 5
    probe_size: int = 5
    core_min_age_days: int = 30

    def __post_init__(self) -> None:
        if (self.core_size, self.boundary_size, self.probe_size) != (50, 5, 5):
            raise ValueError("bootstrap policy must produce 50 core, 5 boundary, and 5 probes")
        if self.core_min_age_days < 1:
            raise ValueError("core_min_age_days must be positive")


@dataclass(frozen=True, slots=True)
class BootstrapSelection:
    decision: dict[str, Any]
    stages: dict[int, tuple[str, ...]]
    steady_members: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _Candidate:
    symbol: str
    onboard_time_ms: int
    quote_volume: Decimal
    age_days: int


def select_bootstrap_universe(
    exchange_info_bytes: bytes,
    market_tickers_bytes: bytes,
    *,
    generated_at: datetime | None = None,
    policy: BootstrapPolicy | None = None,
) -> BootstrapSelection:
    policy = policy or BootstrapPolicy()
    generated_at = generated_at or datetime.now(UTC)
    _require_utc(generated_at, "generated_at")

    exchange_info = _json_object(exchange_info_bytes, "exchangeInfo")
    ticker_rows = _json_array(market_tickers_bytes, "market tickers")
    as_of_ms = _positive_int(exchange_info.get("serverTime"), "exchangeInfo.serverTime")
    as_of = datetime.fromtimestamp(as_of_ms / 1000, UTC)
    raw_symbols = exchange_info.get("symbols")
    if not isinstance(raw_symbols, list):
        raise ValueError("exchangeInfo.symbols must be an array")

    tickers = _parse_tickers(ticker_rows)
    candidates: list[_Candidate] = []
    excluded: list[dict[str, str]] = []
    seen_symbols: set[str] = set()
    for index, raw_symbol in enumerate(raw_symbols):
        if not isinstance(raw_symbol, dict):
            raise ValueError(f"exchangeInfo.symbols[{index}] must be an object")
        symbol = str(raw_symbol.get("symbol", ""))
        reason = _eligibility_reason(raw_symbol, symbol, policy.quote_asset)
        ticker = tickers.get(symbol)
        if reason is None and ticker is None:
            reason = "missing_ticker"
        if reason is not None:
            excluded.append({"symbol": symbol, "reason": reason})
            continue
        if symbol in seen_symbols:
            raise ValueError(f"exchangeInfo contains duplicate eligible symbol: {symbol}")
        seen_symbols.add(symbol)
        onboard_time_ms = _positive_int(
            raw_symbol.get("onboardDate"), f"{symbol}.onboardDate"
        )
        if onboard_time_ms > as_of_ms:
            excluded.append({"symbol": symbol, "reason": "future_onboard_date"})
            continue
        quote_volume = _quote_volume(ticker, symbol)
        if quote_volume is None:
            excluded.append({"symbol": symbol, "reason": "invalid_quote_volume"})
            continue
        candidates.append(
            _Candidate(
                symbol=symbol,
                onboard_time_ms=onboard_time_ms,
                quote_volume=quote_volume,
                age_days=(as_of_ms - onboard_time_ms) // 86_400_000,
            )
        )

    probes = sorted(
        candidates,
        key=lambda item: (-item.onboard_time_ms, -item.quote_volume, item.symbol),
    )[: policy.probe_size]
    probe_symbols = {item.symbol for item in probes}
    mature = [
        item
        for item in candidates
        if item.symbol not in probe_symbols and item.age_days >= policy.core_min_age_days
    ]
    liquidity_ranked = sorted(mature, key=lambda item: (-item.quote_volume, item.symbol))
    required_mature = policy.core_size + policy.boundary_size
    if len(liquidity_ranked) < required_mature or len(probes) < policy.probe_size:
        raise ValueError("not enough eligible contracts to build the bootstrap universe")
    core = liquidity_ranked[: policy.core_size]
    boundary = liquidity_ranked[policy.core_size : required_mature]

    core_symbols = tuple(item.symbol for item in core)
    boundary_symbols = tuple(item.symbol for item in boundary)
    probe_symbols_ordered = tuple(item.symbol for item in probes)
    stage_members = {
        20: tuple(sorted(core_symbols[:20])),
        40: tuple(sorted(core_symbols[:40])),
        50: tuple(sorted(core_symbols)),
        60: tuple(sorted((*core_symbols, *boundary_symbols, *probe_symbols_ordered))),
    }
    steady_members = tuple(sorted((*core_symbols, *boundary_symbols)))
    selected_bucket = {
        **{symbol: "core" for symbol in core_symbols},
        **{symbol: "boundary" for symbol in boundary_symbols},
        **{symbol: "probe" for symbol in probe_symbols_ordered},
    }
    global_ranked = sorted(candidates, key=lambda item: (-item.quote_volume, item.symbol))
    candidate_rows = [
        {
            "symbol": item.symbol,
            "quote_volume_24h": str(item.quote_volume),
            "onboard_at": datetime.fromtimestamp(item.onboard_time_ms / 1000, UTC).isoformat(),
            "age_days": item.age_days,
            "liquidity_rank": rank,
            "selected_bucket": selected_bucket.get(item.symbol),
        }
        for rank, item in enumerate(global_ranked, start=1)
    ]
    excluded.sort(key=lambda item: (item["reason"], item["symbol"]))
    exclusion_counts: dict[str, int] = {}
    for item in excluded:
        reason = item["reason"]
        exclusion_counts[reason] = exclusion_counts.get(reason, 0) + 1
    stages = [
        {
            "target_size": size,
            "members": list(stage_members[size]),
            "universe_hash": universe_hash(stage_members[size]),
        }
        for size in CANARY_STAGE_SIZES
    ]
    decision: dict[str, Any] = {
        "schema_version": 1,
        "decision_type": "bootstrap",
        "generated_at": generated_at.isoformat(),
        "as_of": as_of.isoformat(),
        "sources": {
            "exchange_info": {
                "url": EXCHANGE_INFO_URL,
                "artifact": "sources/exchange-info.json.gz",
                "payload_sha256": sha256_bytes(exchange_info_bytes),
            },
            "market_tickers": {
                "url": MARKET_TICKERS_URL,
                "artifact": "sources/market-tickers.json.gz",
                "payload_sha256": sha256_bytes(market_tickers_bytes),
            },
        },
        "policy": {
            "contract_type": "PERPETUAL",
            "status": "TRADING",
            "quote_asset": policy.quote_asset,
            "margin_asset": policy.quote_asset,
            "core_size": policy.core_size,
            "boundary_size": policy.boundary_size,
            "probe_size": policy.probe_size,
            "core_min_age_days": policy.core_min_age_days,
            "liquidity_metric": "single_snapshot_24h_quote_volume",
            "probe_order": "newest_onboard_date_then_quote_volume",
            "limitation": (
                "Bootstrap has one 24h snapshot; daily selection must use retained history "
                "before claiming a 7-day liquidity statistic."
            ),
        },
        "counts": {
            "exchange_symbols": len(raw_symbols),
            "ticker_symbols": len(tickers),
            "eligible_symbols": len(candidates),
            "excluded_symbols": len(excluded),
            "exclusion_reasons": dict(sorted(exclusion_counts.items())),
        },
        "buckets": {
            "core": list(core_symbols),
            "boundary": list(boundary_symbols),
            "probe": list(probe_symbols_ordered),
        },
        "stages": stages,
        "steady_state": {
            "target_size": len(steady_members),
            "members": list(steady_members),
            "universe_hash": universe_hash(steady_members),
            "reserved_probe_slots": policy.probe_size,
        },
        "candidates": candidate_rows,
        "excluded": excluded,
    }
    return BootstrapSelection(
        decision=decision,
        stages=stage_members,
        steady_members=steady_members,
    )


def write_bootstrap_bundle(
    selection: BootstrapSelection,
    output_dir: Path,
    *,
    exchange_info_bytes: bytes,
    market_tickers_bytes: bytes,
) -> None:
    atomic_write_bytes(
        output_dir / "decision.json", canonical_json_bytes(selection.decision), mode=0o644
    )
    atomic_write_bytes(
        output_dir / "sources" / "exchange-info.json.gz",
        gzip.compress(exchange_info_bytes, mtime=0),
        mode=0o644,
    )
    atomic_write_bytes(
        output_dir / "sources" / "market-tickers.json.gz",
        gzip.compress(market_tickers_bytes, mtime=0),
        mode=0o644,
    )
    for size in CANARY_STAGE_SIZES:
        members = selection.stages[size]
        atomic_write_bytes(
            output_dir / f"stage-{size}.members.txt",
            ("\n".join(members) + "\n").encode("ascii"),
            mode=0o644,
        )
    atomic_write_bytes(
        output_dir / "steady-55.members.txt",
        ("\n".join(selection.steady_members) + "\n").encode("ascii"),
        mode=0o644,
    )


def _parse_tickers(rows: list[Any]) -> dict[str, dict[str, Any]]:
    tickers: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"market tickers[{index}] must be an object")
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            raise ValueError(f"market tickers[{index}] has no symbol")
        if symbol in tickers:
            raise ValueError(f"market tickers contains duplicate symbol: {symbol}")
        tickers[symbol] = row
    return tickers


def _eligibility_reason(raw: dict[str, Any], symbol: str, quote_asset: str) -> str | None:
    if raw.get("status") != "TRADING":
        return "not_trading"
    if raw.get("contractType") != "PERPETUAL":
        return "not_perpetual"
    if raw.get("quoteAsset") != quote_asset or raw.get("marginAsset") != quote_asset:
        return "not_target_quote_or_margin_asset"
    if not SYMBOL_PATTERN.fullmatch(symbol):
        return "invalid_symbol"
    return None


def _quote_volume(ticker: dict[str, Any] | None, symbol: str) -> Decimal | None:
    if ticker is None:
        return None
    try:
        value = Decimal(str(ticker["quoteVolume"]))
    except (InvalidOperation, KeyError):
        return None
    if not value.is_finite() or value < 0:
        return None
    return value


def _json_object(raw: bytes, label: str) -> dict[str, Any]:
    value: Any = orjson.loads(raw)
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def _json_array(raw: bytes, label: str) -> list[Any]:
    value: Any = orjson.loads(raw)
    if not isinstance(value, list):
        raise ValueError(f"{label} must be an array")
    return value


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError(f"{label} must be a positive integer")
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return parsed


def _require_utc(value: datetime, label: str) -> None:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{label} must be UTC")
