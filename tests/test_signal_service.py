import json
from datetime import datetime

from app.services import signal_service


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


def test_to_candles_keeps_volume_when_present():
    """_to_candles giữ thêm volume mỗi phiên (cho cột 'giá trị khớp lệnh' của
    chart Top T+2). Thiếu volume → bỏ qua key, không lỗi (backward compatible)."""
    raw = [
        {"time": 1_700_000_000, "high": "2", "low": "1", "close": "1.5", "volume": "1000"},
        {"time": 1_700_086_400, "high": "3", "low": "2", "close": "2.5"},  # thiếu volume
    ]

    candles = signal_service._to_candles(raw)

    assert candles[0]["volume"] == 1000
    assert "volume" not in candles[1]


def _uptrend_base_with_trailing_buy():
    """60 nến (30 giảm rồi 30 tăng mạnh) → sinh 1 tín hiệu buy ở cuối, đang long."""
    t0 = 1_700_000_000
    base = []
    for i in range(60):
        c = (30 - i * 0.3) if i < 30 else (21 + (i - 30) * 0.5)
        base.append({"time": t0 + i * 86400, "high": c + 0.3, "low": c - 0.3, "close": c})
    return base


def test_live_signal_ignores_not_matched_zero_price_row():
    """Mã chưa khớp lệnh (ATO đầu phiên / thanh khoản thấp) → price_board trả
    match_price NaN → _map_board ép về 0. Nến close=0 KHÔNG phải giá thật; ghép
    vào sẽ kéo SMA20 sập → SELL giả T+0. _live_signal phải trả None để caller
    fallback tín hiệu cache phiên trước."""
    base = _uptrend_base_with_trailing_buy()
    signal_service._history_candles = {"TEST": base}

    row_zero = {"symbol": "TEST", "high": 0, "low": 0, "close": 0}

    assert signal_service._live_signal("TEST", row_zero, "2026-07-22") is None


def test_attach_signals_keeps_prev_signal_when_price_not_matched():
    """Đầu phiên mã chưa khớp (price=0) → panel GIỮ tín hiệu buy phiên trước từ
    cache, không hiển thị SELL giả tại T+0."""
    base = _uptrend_base_with_trailing_buy()
    signal_service._history_candles = {"TEST": base}
    signal_service._cache = {
        "last_refresh": "2026-07-21",
        "signals": {"TEST": {"signal": "buy", "date": "2023-12-20", "price": 23.2}},
    }

    rows = [{"symbol": "TEST", "price": 0, "high": 0, "low": 0, "close": 0}]

    signal_service.attach_signals(
        rows, now=datetime(2026, 7, 22, 9, 8, tzinfo=signal_service.VN_TZ)
    )

    assert rows[0]["signal"] == "buy"
    assert rows[0]["signal_date"] == "2023-12-20"
    assert rows[0]["signal_price"] == 23.2


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