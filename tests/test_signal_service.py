import json
from datetime import datetime

import signal_service


def test_load_history_cache_normalizes_legacy_string_times(tmp_path, monkeypatch):
    history_file = tmp_path / "signal_history.json"
    history_file.write_text(
        json.dumps(
            {
                "AAA": [
                    {"time": "2024-11-28", "high": 2, "low": 1, "close": 1.5},
                    {"time": "1732800000", "high": 3, "low": 2, "close": 2.5},
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(signal_service, "HISTORY_FILE", str(history_file))

    loaded = signal_service.load_history_cache()

    assert isinstance(loaded["AAA"][0]["time"], int)
    assert isinstance(loaded["AAA"][1]["time"], int)
    assert signal_service._candle_date(loaded["AAA"][0]["time"]) == "2024-11-28"


def test_attach_signals_shows_live_signal_during_market_hours(monkeypatch):
    """Tín hiệu cắt TRONG PHIÊN (live phát hôm nay) hiện ngay ở panel, không đợi
    15:05 — ưu tiên live hơn tín hiệu cache cũ dù nến hôm nay còn hình thành."""
    signal_service._cache = {
        "last_refresh": "2026-07-08",
        "signals": {"NVL": {"signal": "sell", "date": "2026-05-06", "price": 16.37}},
    }
    signal_service._history_candles = {}
    monkeypatch.setattr(
        signal_service,
        "_live_signal",
        lambda symbol, row, today: {"signal": "buy", "date": today, "price": 12.4},
    )

    rows = [{"symbol": "NVL", "price": 12850, "high": 12850, "low": 12400, "close": 12850}]

    signal_service.attach_signals(
        rows,
        now=datetime(2026, 7, 8, 10, 0, tzinfo=signal_service.VN_TZ),
    )

    assert rows[0]["signal"] == "buy"
    assert rows[0]["signal_date"] == "2026-07-08"