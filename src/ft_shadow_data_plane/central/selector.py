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

DAY_MS = 86_400_000
TEN_BPS = Decimal("0.001")
FIFTY_BPS = Decimal("0.005")


@dataclass(frozen=True, slots=True)
class RollingPolicy:
    liquidity_window_days: int = 14
    probe_minimum_complete_days: int = 7
    minimum_median_daily_quote_volume: Decimal = Decimal("10000000")
    minimum_q25_daily_quote_volume: Decimal = Decimal("5000000")
    minimum_daily_quote_volume: Decimal = Decimal("3000000")
    maximum_quote_volume_cv: Decimal = Decimal("1.2")
    minimum_median_daily_trades: int = 100_000
    minimum_q25_daily_trades: int = 50_000
    minimum_daily_trades: int = 25_000
    liquidity_depth_samples: int = 3
    liquidity_book_ticker_samples: int = 5
    maximum_spread_bps: Decimal = Decimal("10")
    minimum_thin_depth_10bps: Decimal = Decimal("800")
    minimum_thin_depth_50bps: Decimal = Decimal("10000")
    depth_stable_candidate_count: int = 200
    depth_probe_candidate_count: int = 100
    candidate_minimum_dwell_hours: int = 48
    core_minimum_dwell_days: int = 14
    candidate_daily_replacements: int = 2
    core_weekly_replacements: int = 5
    core_minimum_age_days: int = 30
    core_entry_rank: int = 45
    core_retain_rank: int = 55
    boundary_retain_rank: int = 10
    stable_pool_warning_size: int = 65


@dataclass(frozen=True, slots=True)
class DiscoverySnapshot:
    observed_at: datetime
    exchange_info: bytes
    exchange_info_confirmation: bytes
    market_tickers: bytes
    daily_klines: bytes
    liquidity_depth: bytes

    @property
    def source_hashes(self) -> tuple[str, ...]:
        return tuple(
            sha256_bytes(value)
            for value in (
                self.exchange_info,
                self.exchange_info_confirmation,
                self.market_tickers,
                self.daily_klines,
                self.liquidity_depth,
            )
        )


@dataclass(frozen=True, slots=True)
class SelectionResult:
    core: tuple[str, ...]
    boundary: tuple[str, ...]
    probe: tuple[str, ...]
    inactive: tuple[str, ...]
    source_hashes: tuple[str, ...]
    stable_pool_count: int
    probe_pool_count: int


@dataclass(frozen=True, slots=True)
class _HistoricalRow:
    symbol: str
    onboard_time_ms: int
    age_days: int
    volumes: tuple[Decimal, ...]
    trades: tuple[int, ...]
    expected_probe_days: int
    observed_probe_days: int
    median_quote_volume: Decimal
    q25_quote_volume: Decimal
    minimum_quote_volume: Decimal
    quote_volume_cv: Decimal
    median_trades: Decimal
    q25_trades: Decimal
    minimum_trades: int


@dataclass(frozen=True, slots=True)
class _MarketRow:
    history: _HistoricalRow
    depth_sample_count: int
    maximum_spread_bps: Decimal
    minimum_thin_depth_10bps: Decimal
    minimum_thin_depth_50bps: Decimal
    book_ticker_sample_count: int
    book_ticker_maximum_spread_bps: Decimal

    @property
    def symbol(self) -> str:
        return self.history.symbol

    @property
    def onboard_time_ms(self) -> int:
        return self.history.onboard_time_ms

    @property
    def age_days(self) -> int:
        return self.history.age_days

    @property
    def median_quote_volume(self) -> Decimal:
        return self.history.median_quote_volume

    @property
    def q25_quote_volume(self) -> Decimal:
        return self.history.q25_quote_volume

    @property
    def q25_trades(self) -> Decimal:
        return self.history.q25_trades

    @property
    def quote_volume_cv(self) -> Decimal:
        return self.history.quote_volume_cv


def select_bootstrap_universe(
    snapshot: DiscoverySnapshot,
    *,
    policy: RollingPolicy,
) -> SelectionResult:
    rows, _ = _market_rows(snapshot, tracked=(), policy=policy)
    probe_pool = [
        row
        for row in _probe_pool(rows, policy)
        if len(row.history.volumes) == policy.liquidity_window_days
    ]
    if len(probe_pool) < 5:
        raise ValueError(f"only {len(probe_pool)} probe candidates pass the formal gates")
    probe = tuple(sorted(row.symbol for row in probe_pool[:5]))
    stable_pool = [
        row for row in _stable_pool(rows, policy) if row.symbol not in set(probe)
    ]
    if len(stable_pool) < 55:
        raise ValueError(f"only {len(stable_pool)} stable candidates pass the formal gates")
    core = tuple(sorted(row.symbol for row in stable_pool[:50]))
    boundary = tuple(sorted(row.symbol for row in stable_pool[50:55]))
    return SelectionResult(
        core,
        boundary,
        probe,
        (),
        snapshot.source_hashes,
        len(stable_pool),
        len(probe_pool),
    )


def validate_bootstrap_universe(
    snapshot: DiscoverySnapshot,
    *,
    core: tuple[str, ...],
    boundary: tuple[str, ...],
    probe: tuple[str, ...],
    policy: RollingPolicy,
) -> SelectionResult:
    rows, inactive = _market_rows(
        snapshot,
        tracked=tuple(sorted((*core, *boundary, *probe))),
        policy=policy,
    )
    formal_probe_pool = [
        row
        for row in _probe_pool(rows, policy)
        if len(row.history.volumes) == policy.liquidity_window_days
    ]
    probe_symbols = set(probe)
    stable_pool = [
        row for row in _stable_pool(rows, policy) if row.symbol not in probe_symbols
    ]
    stable_symbols = {row.symbol for row in stable_pool}
    qualified_probe_symbols = {row.symbol for row in formal_probe_pool}
    missing_stable = sorted(set((*core, *boundary)) - stable_symbols)
    missing_probe = sorted(probe_symbols - qualified_probe_symbols)
    if missing_stable or missing_probe:
        raise ValueError(
            "configured generation 1 members fail current formal liquidity gates: "
            f"stable={missing_stable} probe={missing_probe}"
        )
    if len(stable_pool) < 55:
        raise ValueError(f"only {len(stable_pool)} stable candidates pass the formal gates")
    if len(formal_probe_pool) < 5:
        raise ValueError(
            f"only {len(formal_probe_pool)} probe candidates pass the formal gates"
        )
    return SelectionResult(
        core=core,
        boundary=boundary,
        probe=probe,
        inactive=inactive,
        source_hashes=snapshot.source_hashes,
        stable_pool_count=len(stable_pool),
        probe_pool_count=len(formal_probe_pool),
    )


def liquidity_validation_symbols(
    exchange_info: bytes,
    daily_klines: bytes,
    *,
    policy: RollingPolicy,
) -> tuple[str, ...]:
    info = _symbol_info(exchange_info)
    eligible = {
        symbol: raw
        for symbol, raw in info.items()
        if _eligibility_reason(raw, symbol) is None
    }
    klines, cutoff_ms = _daily_klines(daily_klines)
    histories = _historical_rows(
        eligible,
        klines,
        cutoff_ms=cutoff_ms,
        policy=policy,
    )
    stable = sorted(
        (
            row
            for row in histories
            if row.age_days >= policy.core_minimum_age_days
            and len(row.volumes) == policy.liquidity_window_days
            and _volume_gate(row, policy)
        ),
        key=_stable_sort_key,
    )[: policy.depth_stable_candidate_count]
    probes = sorted(
        (
            row
            for row in histories
            if row.expected_probe_days >= policy.probe_minimum_complete_days
            and row.observed_probe_days == row.expected_probe_days
            and _volume_gate(row, policy)
        ),
        key=lambda row: (-row.onboard_time_ms, -row.q25_quote_volume, row.symbol),
    )[: policy.depth_probe_candidate_count]
    return tuple(sorted({row.symbol for row in (*stable, *probes)}))


def select_rolling_universe(
    active: UniverseDecisionV1,
    snapshot: DiscoverySnapshot,
    *,
    effective_at: datetime,
    member_since: dict[str, datetime],
    core_since: dict[str, datetime],
    policy: RollingPolicy,
) -> SelectionResult:
    rows, inactive = _market_rows(snapshot, tracked=active.members, policy=policy)
    stable_pool = _stable_pool(rows, policy)
    stable_rank = {row.symbol: index for index, row in enumerate(stable_pool, start=1)}
    probe_pool = _probe_pool(rows, policy)
    if len(probe_pool) < 5:
        active_probe = set(active.probe)
        return SelectionResult(
            core=active.core,
            boundary=active.boundary,
            probe=active.probe,
            inactive=tuple(sorted(inactive)),
            source_hashes=snapshot.source_hashes,
            stable_pool_count=sum(row.symbol not in active_probe for row in stable_pool),
            probe_pool_count=len(probe_pool),
        )

    core = list(active.core)
    replacement_pool = [row.symbol for row in stable_pool if row.symbol not in core]
    for symbol in (symbol for symbol in core if symbol in inactive):
        replacement = _take_first(replacement_pool, forbidden=set(core))
        if replacement is None:
            raise ValueError("not enough qualified stable instruments to replace inactive core")
        core[core.index(symbol)] = replacement

    if effective_at.weekday() == 0:
        changes = len(set(active.core) - set(core))
        promotable = [
            symbol
            for symbol in replacement_pool
            if symbol not in core
            and stable_rank.get(symbol, 10**9) <= policy.core_entry_rank
        ]
        while promotable and changes < policy.core_weekly_replacements:
            challenger = promotable.pop(0)
            removable = [
                symbol
                for symbol in core
                if stable_rank.get(symbol, 10**9) > policy.core_retain_rank
                and _dwell_complete(
                    core_since.get(symbol, active.effective_at),
                    effective_at,
                    timedelta(days=policy.core_minimum_dwell_days),
                )
            ]
            if not removable:
                break
            incumbent = max(removable, key=lambda symbol: stable_rank.get(symbol, 10**9))
            if stable_rank.get(challenger, 10**9) >= stable_rank.get(incumbent, 10**9):
                break
            core[core.index(incumbent)] = challenger
            changes += 1

    core_set = set(core)
    probe_preferred = [row.symbol for row in probe_pool if row.symbol not in core_set]
    if len(probe_preferred) < 5:
        active_probe = set(active.probe)
        return SelectionResult(
            core=active.core,
            boundary=active.boundary,
            probe=active.probe,
            inactive=tuple(sorted(inactive)),
            source_hashes=snapshot.source_hashes,
            stable_pool_count=sum(row.symbol not in active_probe for row in stable_pool),
            probe_pool_count=len(probe_preferred),
        )
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
    stable_pool_count = sum(row.symbol not in probe_set for row in stable_pool)
    if stable_pool_count < 55:
        return SelectionResult(
            core=active.core,
            boundary=active.boundary,
            probe=active.probe,
            inactive=tuple(sorted(inactive)),
            source_hashes=snapshot.source_hashes,
            stable_pool_count=stable_pool_count,
            probe_pool_count=len(probe_preferred),
        )
    boundary_ranked = [
        row.symbol
        for row in stable_pool
        if row.symbol not in core_set and row.symbol not in probe_set
    ]
    boundary_rank = {
        symbol: index for index, symbol in enumerate(boundary_ranked, start=1)
    }
    protected_boundary = sorted(
        (
            symbol
            for symbol in active.boundary
            if symbol not in inactive
            and symbol not in core_set
            and symbol not in probe_set
            and boundary_rank.get(symbol, 10**9) <= policy.boundary_retain_rank
        ),
        key=lambda symbol: boundary_rank[symbol],
    )
    boundary = _reconcile_bucket(
        active.boundary,
        [*protected_boundary, *boundary_ranked],
        forbidden=core_set | probe_set,
        inactive=set(inactive),
        member_since=member_since,
        effective_at=effective_at,
        minimum_dwell=timedelta(hours=policy.candidate_minimum_dwell_hours),
        normal_replacement_limit=max(1, policy.candidate_daily_replacements - 1),
    )
    return SelectionResult(
        core=tuple(sorted(core)),
        boundary=tuple(sorted(boundary)),
        probe=tuple(sorted(probe)),
        inactive=tuple(sorted(inactive)),
        source_hashes=snapshot.source_hashes,
        stable_pool_count=stable_pool_count,
        probe_pool_count=len(probe_preferred),
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
        ("daily-klines.json.gz", snapshot.daily_klines),
        ("liquidity-depth.json.gz", snapshot.liquidity_depth),
    ):
        atomic_write_bytes(
            output_dir / "sources" / name,
            gzip.compress(content, mtime=0),
            mode=0o644,
        )


def _market_rows(
    snapshot: DiscoverySnapshot,
    *,
    tracked: tuple[str, ...],
    policy: RollingPolicy,
) -> tuple[list[_MarketRow], tuple[str, ...]]:
    first_info = _symbol_info(snapshot.exchange_info)
    confirmed_info = _symbol_info(snapshot.exchange_info_confirmation)
    eligible = {
        symbol: confirmation
        for symbol, raw in first_info.items()
        if (confirmation := confirmed_info.get(symbol)) is not None
        and _eligibility_reason(raw, symbol) is None
        and _eligibility_reason(confirmation, symbol) is None
    }
    inactive = tuple(sorted(symbol for symbol in tracked if symbol not in eligible))
    klines, cutoff_ms = _daily_klines(snapshot.daily_klines)
    histories = _historical_rows(
        eligible,
        klines,
        cutoff_ms=cutoff_ms,
        policy=policy,
    )
    depth = _depth_metrics(snapshot.liquidity_depth)
    rows = [
        _MarketRow(
            history,
            *depth.get(
                history.symbol,
                (
                    0,
                    Decimal("Infinity"),
                    Decimal(),
                    Decimal(),
                    0,
                    Decimal("Infinity"),
                ),
            ),
        )
        for history in histories
    ]
    return rows, inactive


def _historical_rows(
    eligible: dict[str, dict[str, Any]],
    klines: dict[str, list[Any]],
    *,
    cutoff_ms: int,
    policy: RollingPolicy,
) -> list[_HistoricalRow]:
    window_start_ms = cutoff_ms - policy.liquidity_window_days * DAY_MS
    expected = tuple(range(window_start_ms, cutoff_ms, DAY_MS))
    expected_set = set(expected)
    rows: list[_HistoricalRow] = []
    for symbol, raw in eligible.items():
        onboard_ms = _positive_int(raw.get("onboardDate"), f"{symbol}.onboardDate")
        bars: dict[int, Any] = {}
        for value in klines.get(symbol, []):
            if not isinstance(value, list) or len(value) < 9:
                raise ValueError(f"invalid daily kline for {symbol}")
            open_ms = _positive_int(value[0], f"{symbol}.openTime", allow_zero=True)
            close_ms = _positive_int(value[6], f"{symbol}.closeTime", allow_zero=True)
            if open_ms in expected_set and close_ms < cutoff_ms:
                bars[open_ms] = value
        opens = tuple(sorted(bars))
        volumes = tuple(_decimal(bars[open_ms][7], f"{symbol}.quoteVolume") for open_ms in opens)
        trades = tuple(
            _positive_int(bars[open_ms][8], f"{symbol}.trades", allow_zero=True)
            for open_ms in opens
        )
        first_full_day = ((onboard_ms + DAY_MS - 1) // DAY_MS) * DAY_MS
        first_probe_day = max(first_full_day, window_start_ms)
        expected_probe = (
            tuple(range(first_probe_day, cutoff_ms, DAY_MS))
            if first_probe_day < cutoff_ms
            else ()
        )
        observed_probe = sum(open_ms in bars for open_ms in expected_probe)
        if not volumes:
            continue
        rows.append(
            _HistoricalRow(
                symbol=symbol,
                onboard_time_ms=onboard_ms,
                age_days=max(0, (cutoff_ms - onboard_ms) // DAY_MS),
                volumes=volumes,
                trades=trades,
                expected_probe_days=len(expected_probe),
                observed_probe_days=observed_probe,
                median_quote_volume=_median(volumes),
                q25_quote_volume=_percentile(values=volumes, numerator=1, denominator=4),
                minimum_quote_volume=min(volumes),
                quote_volume_cv=_coefficient_of_variation(volumes),
                median_trades=_median(tuple(Decimal(value) for value in trades)),
                q25_trades=_percentile(
                    values=tuple(Decimal(value) for value in trades),
                    numerator=1,
                    denominator=4,
                ),
                minimum_trades=min(trades),
            )
        )
    return rows


def _stable_pool(rows: list[_MarketRow], policy: RollingPolicy) -> list[_MarketRow]:
    return sorted(
        (
            row
            for row in rows
            if row.age_days >= policy.core_minimum_age_days
            and len(row.history.volumes) == policy.liquidity_window_days
            and _quality_gate(row, policy)
        ),
        key=lambda row: (
            -row.q25_quote_volume,
            -row.median_quote_volume,
            -row.q25_trades,
            row.quote_volume_cv,
            row.symbol,
        ),
    )


def _probe_pool(rows: list[_MarketRow], policy: RollingPolicy) -> list[_MarketRow]:
    return sorted(
        (
            row
            for row in rows
            if row.history.expected_probe_days >= policy.probe_minimum_complete_days
            and row.history.observed_probe_days == row.history.expected_probe_days
            and _quality_gate(row, policy)
        ),
        key=lambda row: (-row.onboard_time_ms, -row.q25_quote_volume, row.symbol),
    )


def _quality_gate(row: _MarketRow, policy: RollingPolicy) -> bool:
    return (
        _volume_gate(row.history, policy)
        and row.depth_sample_count == policy.liquidity_depth_samples
        and row.maximum_spread_bps <= policy.maximum_spread_bps
        and row.minimum_thin_depth_10bps >= policy.minimum_thin_depth_10bps
        and row.minimum_thin_depth_50bps >= policy.minimum_thin_depth_50bps
        and row.book_ticker_sample_count == policy.liquidity_book_ticker_samples
        and row.book_ticker_maximum_spread_bps <= policy.maximum_spread_bps
    )


def _volume_gate(row: _HistoricalRow, policy: RollingPolicy) -> bool:
    return (
        row.median_quote_volume >= policy.minimum_median_daily_quote_volume
        and row.q25_quote_volume >= policy.minimum_q25_daily_quote_volume
        and row.minimum_quote_volume >= policy.minimum_daily_quote_volume
        and row.quote_volume_cv <= policy.maximum_quote_volume_cv
        and row.median_trades >= policy.minimum_median_daily_trades
        and row.q25_trades >= policy.minimum_q25_daily_trades
        and row.minimum_trades >= policy.minimum_daily_trades
    )


def _stable_sort_key(
    row: _HistoricalRow,
) -> tuple[Decimal, Decimal, Decimal, Decimal, str]:
    return (
        -row.q25_quote_volume,
        -row.median_quote_volume,
        -row.q25_trades,
        row.quote_volume_cv,
        row.symbol,
    )


def _depth_metrics(
    raw: bytes,
) -> dict[str, tuple[int, Decimal, Decimal, Decimal, int, Decimal]]:
    payload = orjson.loads(raw)
    if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), dict):
        raise ValueError("liquidity depth evidence must contain a symbols object")
    book_samples = payload.get("book_tickers")
    if not isinstance(book_samples, list):
        raise ValueError("liquidity depth evidence must contain book_tickers")
    book_spreads: dict[str, list[Decimal]] = {}
    for sample in book_samples:
        if not isinstance(sample, dict) or not isinstance(sample.get("payload"), list):
            raise ValueError("invalid book ticker evidence")
        for ticker in sample["payload"]:
            if not isinstance(ticker, dict) or not isinstance(ticker.get("symbol"), str):
                raise ValueError("invalid book ticker row")
            bid = _decimal(ticker.get("bidPrice"), "bookTicker.bidPrice")
            ask = _decimal(ticker.get("askPrice"), "bookTicker.askPrice")
            midpoint = (bid + ask) / 2
            book_spreads.setdefault(str(ticker["symbol"]), []).append(
                (ask - bid) / midpoint * Decimal(10_000)
            )

    result: dict[str, tuple[int, Decimal, Decimal, Decimal, int, Decimal]] = {}
    for symbol, samples in payload["symbols"].items():
        if not isinstance(symbol, str) or not isinstance(samples, list):
            raise ValueError("invalid liquidity depth evidence")
        spreads: list[Decimal] = []
        thin_depths: list[Decimal] = []
        thin_depths_50bps: list[Decimal] = []
        for sample in samples:
            if not isinstance(sample, dict) or not isinstance(sample.get("payload"), dict):
                raise ValueError(f"invalid depth sample for {symbol}")
            depth = sample["payload"]
            bids = _book_levels(depth, "bids", symbol)
            asks = _book_levels(depth, "asks", symbol)
            best_bid, best_ask = bids[0][0], asks[0][0]
            midpoint = (best_bid + best_ask) / 2
            spreads.append((best_ask - best_bid) / midpoint * Decimal(10_000))
            bid_floor = midpoint * (1 - TEN_BPS)
            ask_ceiling = midpoint * (1 + TEN_BPS)
            bid_depth = sum(
                (price * quantity for price, quantity in bids if price >= bid_floor),
                Decimal(),
            )
            ask_depth = sum(
                (price * quantity for price, quantity in asks if price <= ask_ceiling),
                Decimal(),
            )
            thin_depths.append(min(bid_depth, ask_depth))
            bid_floor_50bps = midpoint * (1 - FIFTY_BPS)
            ask_ceiling_50bps = midpoint * (1 + FIFTY_BPS)
            bid_depth_50bps = sum(
                (
                    price * quantity
                    for price, quantity in bids
                    if price >= bid_floor_50bps
                ),
                Decimal(),
            )
            ask_depth_50bps = sum(
                (
                    price * quantity
                    for price, quantity in asks
                    if price <= ask_ceiling_50bps
                ),
                Decimal(),
            )
            thin_depths_50bps.append(min(bid_depth_50bps, ask_depth_50bps))
        if spreads:
            ticker_spreads = book_spreads.get(symbol, [])
            result[symbol] = (
                len(spreads),
                max(spreads),
                min(thin_depths),
                min(thin_depths_50bps),
                len(ticker_spreads),
                max(ticker_spreads, default=Decimal("Infinity")),
            )
    return result


def _book_levels(
    payload: dict[str, Any], key: str, symbol: str
) -> list[tuple[Decimal, Decimal]]:
    values = payload.get(key)
    if not isinstance(values, list) or not values:
        raise ValueError(f"depth sample has no {key} for {symbol}")
    result: list[tuple[Decimal, Decimal]] = []
    for value in values:
        if not isinstance(value, list) or len(value) < 2:
            raise ValueError(f"invalid {key} level for {symbol}")
        result.append(
            (
                _decimal(value[0], f"{symbol}.{key}.price"),
                _decimal(value[1], f"{symbol}.{key}.quantity", allow_zero=True),
            )
        )
    return result


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
        raise ValueError("not enough qualified instruments to fill candidate role")
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


def _daily_klines(raw: bytes) -> tuple[dict[str, list[Any]], int]:
    payload = orjson.loads(raw)
    if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), dict):
        raise ValueError("daily kline evidence must contain a symbols object")
    cutoff_ms = _positive_int(
        payload.get("window_end_exclusive_ms"), "dailyKlines.windowEndExclusive"
    )
    if cutoff_ms % DAY_MS:
        raise ValueError("daily kline evidence cutoff must be 00:00 UTC")
    result: dict[str, list[Any]] = {}
    for symbol, value in payload["symbols"].items():
        if (
            not isinstance(symbol, str)
            or not isinstance(value, dict)
            or not isinstance(value.get("payload"), list)
        ):
            raise ValueError("invalid daily kline evidence")
        result[symbol] = value["payload"]
    return result, cutoff_ms


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


def _positive_int(value: object, label: str, *, allow_zero: bool = False) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be an integer")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be an integer") from exc
    if parsed < 0 or (parsed == 0 and not allow_zero):
        raise ValueError(f"{label} must be {'non-negative' if allow_zero else 'positive'}")
    return parsed


def _decimal(value: object, label: str, *, allow_zero: bool = False) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{label} must be a decimal") from exc
    if not parsed.is_finite() or parsed < 0 or (parsed == 0 and not allow_zero):
        raise ValueError(f"{label} must be {'non-negative' if allow_zero else 'positive'}")
    return parsed


def _median(values: tuple[Decimal, ...]) -> Decimal:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2


def _percentile(
    *, values: tuple[Decimal, ...], numerator: int, denominator: int
) -> Decimal:
    ordered = sorted(values)
    position_numerator = (len(ordered) - 1) * numerator
    lower = position_numerator // denominator
    remainder = position_numerator % denominator
    if remainder == 0:
        return ordered[lower]
    upper = lower + 1
    return (
        ordered[lower] * (denominator - remainder) + ordered[upper] * remainder
    ) / denominator


def _coefficient_of_variation(values: tuple[Decimal, ...]) -> Decimal:
    mean = sum(values, Decimal()) / len(values)
    if mean == 0:
        return Decimal("Infinity")
    variance = sum(((value - mean) ** 2 for value in values), Decimal()) / len(values)
    return variance.sqrt() / mean
