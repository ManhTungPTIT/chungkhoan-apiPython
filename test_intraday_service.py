"""Test intraday_service offline — inject fetch_fn giả + now cố định, không gọi mạng."""

from datetime import datetime, timedelta

import intraday_service


def _capture():
    """fetch_fn giả: ghi lại tham số gọi, trả về list nến rỗng-giả."""
    calls = []

    def fetch_fn(symbol, start, end, interval):
        calls.append({"symbol": symbol, "start": start, "end": end, "interval": interval})
        return [{"time": "2026-06-22", "open": "1", "high": "1", "low": "1", "close": "1"}]

    return calls, fetch_fn


NOW = datetime(2026, 6, 22, tzinfo=intraday_service.VN_TZ)


def test_intraday_intraday_interval_uses_lookback_and_maps_vnstock():
    """1h → vnstock '1H', start = hôm nay − 90 ngày, end = hôm nay."""
    calls, fetch_fn = _capture()
    out = intraday_service.get_intraday("TCB", interval="1h", fetch_fn=fetch_fn, now=NOW)

    assert len(calls) == 1
    c = calls[0]
    assert c["symbol"] == "TCB"
    assert c["interval"] == "1H"
    assert c["start"] == (NOW - timedelta(days=90)).strftime("%Y-%m-%d")
    assert c["end"] == "2026-06-22"
    assert out["data"]  # truyền qua nguyên vẹn


def test_intraday_daily_uses_fixed_start():
    """1d → vnstock '1D', start cố định 2025-01-01."""
    calls, fetch_fn = _capture()
    intraday_service.get_intraday("TCB", interval="1d", fetch_fn=fetch_fn, now=NOW)
    assert calls[0]["interval"] == "1D"
    assert calls[0]["start"] == "2025-01-01"


def test_intraday_unknown_interval_falls_back_to_daily():
    """Interval lạ (2h không hỗ trợ) → fallback 1d."""
    calls, fetch_fn = _capture()
    intraday_service.get_intraday("TCB", interval="2h", fetch_fn=fetch_fn, now=NOW)
    assert calls[0]["interval"] == "1D"
    assert calls[0]["start"] == "2025-01-01"


def test_intraday_none_returns_empty():
    """fetch_fn trả None → {data: []}."""
    out = intraday_service.get_intraday(
        "TCB", interval="1d", fetch_fn=lambda *a, **k: None, now=NOW
    )
    assert out == {"data": []}
