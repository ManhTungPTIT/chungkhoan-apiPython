"""Test period_gain_service — chart TOP TĂNG TUẦN (và sau này THÁNG).

Toàn board sắp theo % tăng trong kỳ. Mỗi mã 3 số:
  - gia_tri_khop_lenh (Tỷ): SUM value các phiên trong kỳ (phiên đã đóng từ history
    volume×close×1000 + phiên hôm nay lấy value live board)
  - gia_hien_tai (Nghìn): giá live / 1000
  - pct_tang (%): (giá hiện tại − close phiên ĐẦU kỳ) / close đầu kỳ × 100
"""

from datetime import date, datetime

from app.services import period_gain_service
from app.services import signal_service

NOW = datetime(2026, 7, 22, 10, 0, tzinfo=signal_service.VN_TZ)  # thứ Tư


def _epoch(iso):
    return int(
        datetime.strptime(iso, "%Y-%m-%d")
        .replace(tzinfo=signal_service.VN_TZ)
        .timestamp()
    )


AAA_CANDLES = [
    {"time": _epoch("2026-07-17"), "high": 18.5, "low": 17.5, "close": 18.0, "volume": 500},   # tuần trước → loại
    {"time": _epoch("2026-07-20"), "high": 20.5, "low": 19.5, "close": 20.0, "volume": 1000},  # T2 = đầu tuần
    {"time": _epoch("2026-07-21"), "high": 22.5, "low": 21.5, "close": 22.0, "volume": 2000},  # T3
]


def _row(symbol, price, value):
    return {"symbol": symbol, "price": price, "value": value}


def test_week_start_is_monday():
    assert period_gain_service.period_start("week", NOW) == date(2026, 7, 20)


def test_month_start_is_first_day():
    assert period_gain_service.period_start("month", NOW) == date(2026, 7, 1)


def test_computes_weekly_value_price_and_pct():
    board = [_row("AAA", 24000, 3_000_000_000)]
    start = period_gain_service.period_start("week", NOW)

    result = period_gain_service.compute_period_gain(
        board, {"AAA": AAA_CANDLES}, start, now=NOW
    )

    row = result["rows"][0]
    assert row["gia_hien_tai"] == 24.0
    assert row["pct_tang"] == 20.0                          # (24-20)/20*100, đầu tuần close=20
    # value = T2 (20×1000×1000) + T3 (22×2000×1000) + hôm nay live (3e9) = 3.064 Tỷ
    assert row["gia_tri_khop_lenh"] == 3.064


def test_sorts_desc_top_n_and_skips_missing():
    board = [
        _row("LOW", 21000, 1e9),    # đầu tuần 20 → +5%
        _row("HIGH", 24000, 1e9),   # +20%
        _row("NOHIST", 30000, 1e9), # thiếu history → loại
    ]
    history = {"LOW": AAA_CANDLES, "HIGH": AAA_CANDLES}

    result = period_gain_service.compute_period_gain(
        board, history, period_gain_service.period_start("week", NOW), now=NOW, top_n=1
    )

    assert [r["symbol"] for r in result["rows"]] == ["HIGH"]


def test_skips_symbol_without_session_in_period():
    """Mã chỉ có nến TRƯỚC đầu kỳ (không có phiên nào trong kỳ) → không tính được
    close đầu kỳ → loại."""
    old_only = [{"time": _epoch("2026-07-10"), "high": 9, "low": 8, "close": 8.5, "volume": 100}]
    result = period_gain_service.compute_period_gain(
        [_row("OLD", 24000, 1e9)],
        {"OLD": old_only},
        period_gain_service.period_start("week", NOW),
        now=NOW,
    )
    assert result["rows"] == []
