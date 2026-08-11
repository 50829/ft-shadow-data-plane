from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ft_shadow_data_plane.central.process_cli import _finalize


def test_finalize_rejects_empty_l2_validity(tmp_path: Path) -> None:
    utc_date = date(2026, 8, 10)
    typed_root = tmp_path / "typed/collector=tokyo01/date=2026-08-10"
    typed_root.mkdir(parents=True)
    (typed_root / "_NORMALIZED.json").write_text("{}", encoding="ascii")
    symbol_root = tmp_path / "quality/collector=tokyo01/date=2026-08-10/symbol=BTCUSDT"
    symbol_root.mkdir(parents=True)
    (symbol_root / "l2-validity.jsonl").write_bytes(b"")
    (symbol_root / "l2-checkpoint.json").write_text(
        '{"schema_version":1,"collector_id":"tokyo01","utc_date":"2026-08-10",'
        '"exchange_symbol":"BTCUSDT","state":"UNANCHORED","connection_id":null,'
        '"previous_update_id":null,"valid_through_ns":null,"bids":[],"asks":[]}\n',
        encoding="ascii",
    )

    with pytest.raises(ValueError, match="empty L2 validity"):
        _finalize(
            tmp_path,
            collector_id="tokyo01",
            utc_date=utc_date,
            symbols=("BTCUSDT",),
        )

    assert not (
        tmp_path / "quality/collector=tokyo01/date=2026-08-10/_PROCESSED.json"
    ).exists()
