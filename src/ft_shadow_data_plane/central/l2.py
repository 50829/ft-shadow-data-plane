from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from ft_shadow_data_plane.contracts.models import GapEventV1, GapState, StreamType
from ft_shadow_data_plane.contracts.serde import atomic_write_bytes, canonical_json_bytes


class L2State(StrEnum):
    UNANCHORED = "UNANCHORED"
    VALID = "VALID"
    GAPPED = "GAPPED"


@dataclass(frozen=True, slots=True)
class DepthDiff:
    connection_id: str
    receive_seq: int
    received_ns: int
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int
    payload_hash: bytes
    bids: tuple[tuple[str, str], ...]
    asks: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class DepthSnapshot:
    connection_id: str
    receive_seq: int
    received_ns: int
    last_update_id: int
    bids: tuple[tuple[str, str], ...]
    asks: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class StateChange:
    connection_id: str
    at_ns: int
    state: L2State
    update_id: int | None
    reason: str


@dataclass(slots=True)
class ConnectionBook:
    connection_id: str
    state: L2State = L2State.UNANCHORED
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    previous_update_id: int | None = None
    pending: list[DepthDiff] = field(default_factory=list)
    seen_diffs: set[tuple[int, int, int, bytes]] = field(default_factory=set)
    anchor_last_update_id: int | None = None
    anchor_received_ns: int | None = None

    def on_diff(self, diff: DepthDiff) -> StateChange | None:
        identity = (
            diff.first_update_id,
            diff.final_update_id,
            diff.previous_final_update_id,
            diff.payload_hash,
        )
        if identity in self.seen_diffs:
            return None
        self.seen_diffs.add(identity)
        if self.state is not L2State.VALID:
            self.pending.append(diff)
            return self._try_bridge()
        if diff.previous_final_update_id != self.previous_update_id:
            self.state = L2State.GAPPED
            self.bids.clear()
            self.asks.clear()
            self.previous_update_id = None
            self.pending = [diff]
            self.anchor_last_update_id = None
            self.anchor_received_ns = None
            return StateChange(
                self.connection_id,
                diff.received_ns,
                L2State.GAPPED,
                diff.final_update_id,
                "pu_discontinuity",
            )
        self._apply(diff)
        return None

    def on_snapshot(self, snapshot: DepthSnapshot) -> StateChange | None:
        self.bids = _book_side(snapshot.bids)
        self.asks = _book_side(snapshot.asks)
        self.previous_update_id = snapshot.last_update_id
        self.anchor_last_update_id = snapshot.last_update_id
        self.anchor_received_ns = snapshot.received_ns
        return self._try_bridge()

    def _try_bridge(self) -> StateChange | None:
        if self.anchor_last_update_id is None or self.anchor_received_ns is None:
            return None
        candidates = sorted(self.pending, key=lambda diff: diff.receive_seq)
        candidates = [
            diff for diff in candidates if diff.final_update_id >= self.anchor_last_update_id
        ]
        bridge_index = next(
            (
                index
                for index, diff in enumerate(candidates)
                if diff.first_update_id
                <= self.anchor_last_update_id
                <= diff.final_update_id
            ),
            None,
        )
        if bridge_index is None:
            self.pending = candidates
            return None

        bridge = candidates[bridge_index]
        self._apply(bridge)
        for diff in candidates[bridge_index + 1 :]:
            if diff.previous_final_update_id != self.previous_update_id:
                self.state = L2State.GAPPED
                self.bids.clear()
                self.asks.clear()
                self.previous_update_id = None
                self.pending = [diff]
                self.anchor_last_update_id = None
                self.anchor_received_ns = None
                return StateChange(
                    self.connection_id,
                    diff.received_ns,
                    L2State.GAPPED,
                    diff.final_update_id,
                    "buffered_pu_discontinuity",
                )
            self._apply(diff)
        self.pending.clear()
        self.state = L2State.VALID
        valid_at = max(self.anchor_received_ns, bridge.received_ns)
        self.anchor_last_update_id = None
        self.anchor_received_ns = None
        return StateChange(
            self.connection_id,
            valid_at,
            L2State.VALID,
            self.previous_update_id,
            "snapshot_bridge",
        )

    def _apply(self, diff: DepthDiff) -> None:
        _apply_levels(self.bids, diff.bids)
        _apply_levels(self.asks, diff.asks)
        self.previous_update_id = diff.final_update_id


class L2DayReconstructor:
    def __init__(
        self,
        *,
        derived_root: Path,
        collector_id: str,
        utc_date: date,
        exchange_symbol: str,
    ) -> None:
        self._typed_root = (
            derived_root
            / "typed"
            / f"collector={collector_id}"
            / f"date={utc_date.isoformat()}"
        )
        self._quality_root = (
            derived_root
            / "quality"
            / f"collector={collector_id}"
            / f"date={utc_date.isoformat()}"
            / f"symbol={exchange_symbol}"
        )
        self._transport_gap_path = (
            derived_root
            / "quality"
            / f"collector={collector_id}"
            / f"date={utc_date.isoformat()}"
            / "transport-gaps.jsonl"
        )
        self._date = utc_date
        self._symbol = exchange_symbol

    def run(self) -> tuple[int, int]:
        books: dict[str, ConnectionBook] = {}
        changes: list[StateChange] = []
        intervals: list[dict[str, Any]] = []
        authority: str | None = None
        valid_from: int | None = None

        for row in self._depth_rows():
            connection_id = str(row["connection_id"])
            book = books.setdefault(connection_id, ConnectionBook(connection_id))
            stream = StreamType(str(row["stream_type"]))
            if stream is StreamType.DEPTH:
                change = book.on_diff(_diff_from_row(row))
            else:
                change = book.on_snapshot(_snapshot_from_row(row))
            if change is None:
                continue
            changes.append(change)
            if change.state is L2State.VALID:
                if authority is not None and valid_from is not None:
                    intervals.append(
                        _interval(valid_from, change.at_ns, authority, "connection_switch")
                    )
                authority = connection_id
                valid_from = change.at_ns
            elif authority == connection_id and valid_from is not None:
                intervals.append(_interval(valid_from, change.at_ns, authority, change.reason))
                authority = None
                valid_from = None

        end_ns = int(
            datetime.combine(
                self._date + timedelta(days=1), datetime.min.time(), UTC
            ).timestamp()
            * 1_000_000_000
        )
        if authority is not None and valid_from is not None:
            intervals.append(_interval(valid_from, end_ns, authority, "utc_day_end"))
        intervals = self._truncate_at_transport_gaps(intervals)
        self._write(changes, intervals)
        return len(changes), len(intervals)

    def _depth_rows(self) -> Any:
        files = sorted(self._typed_root.glob("*.typed.parquet"))
        columns = [
            "exchange_symbol",
            "stream_type",
            "connection_id",
            "receive_seq",
            "app_receive_realtime_ns",
            "payload_hash",
            "first_update_id",
            "final_update_id",
            "previous_final_update_id",
            "last_update_id",
            "bids",
            "asks",
        ]
        for path in files:
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches(batch_size=10_000, columns=columns):
                for row in batch.to_pylist():
                    if row["exchange_symbol"] != self._symbol:
                        continue
                    if row["stream_type"] not in {
                        StreamType.DEPTH.value,
                        StreamType.DEPTH_SNAPSHOT.value,
                    }:
                        continue
                    yield row

    def _truncate_at_transport_gaps(
        self, intervals: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if not self._transport_gap_path.exists():
            return intervals
        gap_times = []
        with self._transport_gap_path.open("rb") as source:
            for line in source:
                event = GapEventV1.model_validate_json(line)
                if event.state is not GapState.OPEN:
                    continue
                if event.exchange_symbols and self._symbol not in event.exchange_symbols:
                    continue
                if event.stream_types and StreamType.DEPTH not in event.stream_types:
                    continue
                gap_times.append(event.observed_at_realtime_ns)
        for interval in intervals:
            candidates = [
                moment
                for moment in gap_times
                if interval["valid_from_ns"] < moment < interval["valid_to_ns"]
            ]
            if candidates:
                interval["valid_to_ns"] = min(candidates)
                interval["end_reason"] = "transport_gap"
        return [
            interval
            for interval in intervals
            if interval["valid_to_ns"] > interval["valid_from_ns"]
        ]

    def _write(self, changes: list[StateChange], intervals: list[dict[str, Any]]) -> None:
        change_rows = [
            {
                "schema_version": 1,
                "exchange_symbol": self._symbol,
                "connection_id": change.connection_id,
                "at_ns": change.at_ns,
                "state": change.state.value,
                "update_id": change.update_id,
                "reason": change.reason,
            }
            for change in changes
        ]
        atomic_write_bytes(
            self._quality_root / "connection-states.jsonl",
            b"".join(canonical_json_bytes(row) for row in change_rows),
        )
        atomic_write_bytes(
            self._quality_root / "l2-validity.jsonl",
            b"".join(canonical_json_bytes(row) for row in intervals),
        )


def _diff_from_row(row: dict[str, Any]) -> DepthDiff:
    return DepthDiff(
        connection_id=str(row["connection_id"]),
        receive_seq=int(row["receive_seq"]),
        received_ns=int(row["app_receive_realtime_ns"]),
        first_update_id=int(row["first_update_id"]),
        final_update_id=int(row["final_update_id"]),
        previous_final_update_id=int(row["previous_final_update_id"]),
        payload_hash=bytes(row["payload_hash"]),
        bids=_row_levels(row["bids"]),
        asks=_row_levels(row["asks"]),
    )


def _snapshot_from_row(row: dict[str, Any]) -> DepthSnapshot:
    return DepthSnapshot(
        connection_id=str(row["connection_id"]),
        receive_seq=int(row["receive_seq"]),
        received_ns=int(row["app_receive_realtime_ns"]),
        last_update_id=int(row["last_update_id"]),
        bids=_row_levels(row["bids"]),
        asks=_row_levels(row["asks"]),
    )


def _row_levels(values: list[dict[str, str]]) -> tuple[tuple[str, str], ...]:
    return tuple((value["price"], value["quantity"]) for value in values)


def _book_side(levels: tuple[tuple[str, str], ...]) -> dict[Decimal, Decimal]:
    return {
        Decimal(price): Decimal(quantity)
        for price, quantity in levels
        if Decimal(quantity) != 0
    }


def _apply_levels(
    side: dict[Decimal, Decimal], levels: tuple[tuple[str, str], ...]
) -> None:
    for price_text, quantity_text in levels:
        price = Decimal(price_text)
        quantity = Decimal(quantity_text)
        if quantity == 0:
            side.pop(price, None)
        else:
            side[price] = quantity


def _interval(start: int, end: int, connection_id: str, reason: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "valid_from_ns": start,
        "valid_to_ns": end,
        "connection_id": connection_id,
        "end_reason": reason,
    }
