from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ft_shadow_data_plane.central.l2 import (
    ConnectionBook,
    DepthDiff,
    DepthSnapshot,
    L2DayReconstructor,
    L2State,
)
from ft_shadow_data_plane.contracts.typed_schema import TYPED_EVENT_SCHEMA_V1


def _diff(
    sequence: int, first: int, final: int, previous: int, *, connection: str = "a"
) -> DepthDiff:
    return DepthDiff(
        connection,
        sequence,
        sequence * 100,
        first,
        final,
        previous,
        bytes([sequence]) * 32,
        (("100", "1"),),
        (("101", "1"),),
    )


def test_snapshot_bridge_duplicate_gap_and_reanchor() -> None:
    future_bridge = ConnectionBook("future")
    assert future_bridge.on_snapshot(
        DepthSnapshot("future", 1, 100, 100, (("99", "1"),), (("102", "1"),))
    ) is None
    future_change = future_bridge.on_diff(
        _diff(2, 99, 101, 98, connection="future")
    )
    assert future_change is not None and future_change.state is L2State.VALID

    book = ConnectionBook("a")
    first = _diff(1, 100, 101, 99)
    assert book.on_diff(first) is None
    change = book.on_snapshot(
        DepthSnapshot("a", 2, 200, 100, (("99", "1"),), (("102", "1"),))
    )
    assert change.state is L2State.VALID
    assert book.previous_update_id == 101
    assert book.on_diff(first) is None

    change = book.on_diff(_diff(3, 102, 102, 999))
    assert change is not None and change.state is L2State.GAPPED
    assert book.on_diff(_diff(4, 103, 104, 102)) is None
    change = book.on_snapshot(
        DepthSnapshot("a", 5, 500, 103, (("99", "1"),), (("102", "1"),))
    )
    assert change.state is L2State.VALID
    assert book.previous_update_id == 104


def test_new_anchored_connection_is_explicit_authority_boundary(tmp_path: Path) -> None:
    typed_root = tmp_path / "typed" / "collector=tokyo01" / "date=2026-08-10"
    typed_root.mkdir(parents=True)
    rows = [
        _typed_depth("a", 1, 100, 100, 101, 99),
        _typed_snapshot("a", 2, 200, 100),
        _typed_depth("b", 1, 300, 200, 201, 199),
        _typed_snapshot("b", 2, 400, 200),
    ]
    pq.write_table(
        pa.Table.from_pylist(rows, schema=TYPED_EVENT_SCHEMA_V1),
        typed_root / "depth.typed.parquet",
    )
    changes, intervals = L2DayReconstructor(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=date(2026, 8, 10),
        exchange_symbol="BTCUSDT",
    ).run()
    assert changes == 2
    assert intervals == 2
    lines = (
        tmp_path
        / "quality/collector=tokyo01/date=2026-08-10/symbol=BTCUSDT/l2-validity.jsonl"
    ).read_text().splitlines()
    assert '"connection_id":"a"' in lines[0]
    assert '"valid_to_ns":400' in lines[0]
    assert '"connection_id":"b"' in lines[1]


def _typed_depth(
    connection: str, sequence: int, received: int, first: int, final: int, previous: int
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "exchange_symbol": "BTCUSDT",
        "stream_type": "depth",
        "connection_id": connection,
        "receive_seq": sequence,
        "app_receive_realtime_ns": received,
        "app_receive_monotonic_ns": received,
        "payload_hash": bytes([sequence]) * 32,
        "is_duplicate": False,
        "first_update_id": first,
        "final_update_id": final,
        "previous_final_update_id": previous,
        "bids": [{"price": "100", "quantity": "1"}],
        "asks": [{"price": "101", "quantity": "1"}],
    }


def _typed_snapshot(
    connection: str, sequence: int, received: int, last_update_id: int
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "exchange_symbol": "BTCUSDT",
        "stream_type": "depth_snapshot",
        "connection_id": connection,
        "receive_seq": sequence,
        "app_receive_realtime_ns": received,
        "app_receive_monotonic_ns": received,
        "payload_hash": bytes([sequence]) * 32,
        "is_duplicate": False,
        "last_update_id": last_update_id,
        "bids": [{"price": "99", "quantity": "1"}],
        "asks": [{"price": "102", "quantity": "1"}],
    }
