"""Test top_gain_service — hàm thuần compute_top_gain, không gọi mạng.

Chart "TOP TĂNG CAO NHẤT T+2": lấy mã đang HOLD đúng T+2 (signal_hold=True,
signal_sessions==2), mỗi mã 3 số:
  - gia_tri_khop_lenh (Tỷ): value 2 phiên gần nhất (T+1 từ history + T+2 live board)
  - gia_hien_tai (Nghìn): giá live (board price / 1000)
  - pct_tang (%): (giá hiện tại − close ngày báo T0) / close T0 × 100
"""

from datetime import datetime

from app.services import signal_service
from app.services import top_gain_service

NOW = datetime(2026, 7, 22, 10, 0, tzinfo=signal_service.VN_TZ)


def _epoch(iso):
    return int(
        datetime.strptime(iso, "%Y-%m-%d")
        .replace(tzinfo=signal_service.VN_TZ)
        .timestamp()
    )


def _hold_t2_row(symbol, price, value, signal_date="2026-07-20"):
    return {
        "symbol": symbol,
        "price": price,          # VND live
        "value": value,          # VND value hôm nay (T+2)
        "signal": "buy",
        "signal_date": signal_date,
        "signal_sessions": 2,
        "signal_hold": True,
    }


# AAA: history có T0 (20/07 close=20) + T+1 (21/07 close=21, vol=1000). Hôm nay
# (22/07) là T+2, giá/value lấy từ board row.
AAA_CANDLES = [
    {"time": _epoch("2026-07-20"), "high": 20.5, "low": 19.5, "close": 20.0, "volume": 500},
    {"time": _epoch("2026-07-21"), "high": 21.5, "low": 20.5, "close": 21.0, "volume": 1000},
]


def test_filters_only_hold_tplus2():
    """Chỉ giữ mã signal_hold=True VÀ signal_sessions==2; loại T+3 và loại mã
    không hold (buy hôm nay)."""
    board = [
        _hold_t2_row("AAA", 22000, 3_000_000_000),
        {**_hold_t2_row("BBB", 30000, 1e9), "signal_sessions": 3},  # T+3 → loại
        {**_hold_t2_row("CCC", 30000, 1e9), "signal_hold": False},  # không hold → loại
    ]
    history = {"AAA": AAA_CANDLES, "BBB": AAA_CANDLES, "CCC": AAA_CANDLES}

    result = top_gain_service.compute_top_gain(board, history, now=NOW)

    assert [r["symbol"] for r in result["rows"]] == ["AAA"]


def test_computes_price_pct_and_value():
    board = [_hold_t2_row("AAA", 22000, 3_000_000_000)]
    result = top_gain_service.compute_top_gain(board, {"AAA": AAA_CANDLES}, now=NOW)

    row = result["rows"][0]
    assert row["gia_hien_tai"] == 22.0                       # 22000 / 1000
    assert row["pct_tang"] == 10.0                           # (22-20)/20*100
    # value = T+1 (21×1000×1000 = 21,000,000 VND) + T+2 live (3,000,000,000) → Tỷ
    assert row["gia_tri_khop_lenh"] == 3.021


def test_sorts_desc_and_limits_top_n():
    board = [
        _hold_t2_row("LOW", 21000, 1e9),   # +5%
        _hold_t2_row("HIGH", 24000, 1e9),  # +20%
        _hold_t2_row("MID", 22000, 1e9),   # +10%
    ]
    history = {s: AAA_CANDLES for s in ("LOW", "HIGH", "MID")}

    result = top_gain_service.compute_top_gain(board, history, now=NOW, top_n=2)

    assert [r["symbol"] for r in result["rows"]] == ["HIGH", "MID"]


def test_window3_filters_tplus3_and_sums_three_sessions():
    """window=3 (chart T+3): giữ signal_sessions==3, loại T+2; value = 2 phiên đã
    đóng (T+1,T+2) + phiên hôm nay (T+3 live); % so close ngày báo (=3 phiên trước)."""
    candles_t3 = [
        {"time": _epoch("2026-07-17"), "high": 20.5, "low": 19.5, "close": 20.0, "volume": 100},   # T0
        {"time": _epoch("2026-07-20"), "high": 21.5, "low": 20.5, "close": 21.0, "volume": 1000},   # T+1
        {"time": _epoch("2026-07-21"), "high": 22.5, "low": 21.5, "close": 22.0, "volume": 2000},   # T+2
    ]
    board = [
        {**_hold_t2_row("AAA", 24000, 5_000_000_000, signal_date="2026-07-17"), "signal_sessions": 3},
        _hold_t2_row("SKIP", 24000, 1e9),  # signal_sessions==2 → loại khi window=3
    ]
    history = {"AAA": candles_t3, "SKIP": AAA_CANDLES}

    result = top_gain_service.compute_top_gain(board, history, now=NOW, window=3)

    assert [r["symbol"] for r in result["rows"]] == ["AAA"]
    assert result["window"] == 3
    row = result["rows"][0]
    assert row["gia_hien_tai"] == 24.0
    assert row["pct_tang"] == 20.0                          # (24-20)/20*100
    # value = T+1 (21×1000×1000) + T+2 (22×2000×1000) + T+3 live (5e9) = 5.065 Tỷ
    assert row["gia_tri_khop_lenh"] == 5.065


def test_skips_symbol_without_history_or_bad_t0():
    board = [
        _hold_t2_row("NOHIST", 22000, 1e9),
        _hold_t2_row("ZEROT0", 22000, 1e9),
    ]
    zero_t0 = [
        {"time": _epoch("2026-07-20"), "high": 0, "low": 0, "close": 0.0, "volume": 10},
        {"time": _epoch("2026-07-21"), "high": 21.5, "low": 20.5, "close": 21.0, "volume": 1000},
    ]
    result = top_gain_service.compute_top_gain(
        board, {"ZEROT0": zero_t0}, now=NOW
    )

    assert result["rows"] == []
