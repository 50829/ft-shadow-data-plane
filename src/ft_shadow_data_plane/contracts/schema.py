from __future__ import annotations

import pyarrow as pa

from ft_shadow_data_plane.contracts.models import RawEventV1

RAW_EVENT_SCHEMA_V1 = pa.schema(
    [
        pa.field("schema_version", pa.int16(), nullable=False),
        pa.field("exchange_symbol", pa.string()),
        pa.field("stream_type", pa.dictionary(pa.int16(), pa.string()), nullable=False),
        pa.field("collector_id", pa.string(), nullable=False),
        pa.field("boot_id", pa.string(), nullable=False),
        pa.field("segment_id", pa.string(), nullable=False),
        pa.field("connection_id", pa.string(), nullable=False),
        pa.field("receive_seq", pa.int64(), nullable=False),
        pa.field("app_receive_realtime_ns", pa.int64(), nullable=False),
        pa.field("app_receive_monotonic_ns", pa.int64(), nullable=False),
        pa.field("payload_bytes", pa.binary(), nullable=False),
        pa.field("request_id", pa.string()),
        pa.field("request_realtime_ns", pa.int64()),
    ]
)


def raw_events_to_table(events: list[RawEventV1]) -> pa.Table:
    rows = [
        {
            "schema_version": event.schema_version,
            "exchange_symbol": event.exchange_symbol,
            "stream_type": event.stream_type.value,
            "collector_id": event.collector_id,
            "boot_id": event.boot_id,
            "segment_id": event.segment_id,
            "connection_id": event.connection_id,
            "receive_seq": event.receive_seq,
            "app_receive_realtime_ns": event.app_receive_realtime_ns,
            "app_receive_monotonic_ns": event.app_receive_monotonic_ns,
            "payload_bytes": event.payload_bytes,
            "request_id": event.request_id,
            "request_realtime_ns": event.request_realtime_ns,
        }
        for event in events
    ]
    return pa.Table.from_pylist(rows, schema=RAW_EVENT_SCHEMA_V1)
