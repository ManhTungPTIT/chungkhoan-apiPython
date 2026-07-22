"""Test flow_surge_service — chart "DÒNG TIỀN TĂNG ĐỘT BIẾN NỔI BẬT HÔM NAY".

Cột xanh = % tăng DÒNG TIỀN (không phải giá):
  % = (value hôm nay − trung bình value N phiên gần nhất) / trung bình × 100
Cột tím = value RIÊNG hôm nay (không cộng dồn). Đường vàng = giá hiện tại.

Lọc chất lượng (bỏ penny tăng đột biến): mã phải có TB value nền ≥ MIN_BASE và
giá ≥ MIN_PRICE. Nhiều test toán học truyền min_base/min_price = 0 để cô lập.
"""

from datetime import datetime

from app.services import flow_surge_service
from app.data import market_cache
from app.services import signal_service

NOW = datetime(2026, 7, 22, 10, 0, tzinfo=signal_service.VN_TZ)


def _epoch(iso):
    return int(
        datetime.strptime(iso, "%Y-%m-%d")
        .replace(tzinfo=signal_service.VN_TZ)
        .timestamp()
    )


# 2 phiên nền, value mỗi phiên = volume×close×1000:
#   20/07: 20 × 1000 × 1000 = 20,000,000
#   21/07: 22 × 1000 × 1000 = 22,000,000  → trung bình 21,000,000
HIST = [
    {"time": _epoch("2026-07-20"), "high": 20.5, "low": 19.5, "close": 20.0, "volume": 1000},
    {"time": _epoch("2026-07-21"), "high": 22.5, "low": 21.5, "close": 22.0, "volume": 1000},
]

# Nền thanh khoản lớn (≥ 1 tỷ): volume 100k × close 20 × 1000 = 2 tỷ/phiên.
LIQUID_HIST = [
    {"time": _epoch("2026-07-20"), "high": 20, "low": 20, "close": 20.0, "volume": 100_000},
    {"time": _epoch("2026-07-21"), "high": 20, "low": 20, "close": 20.0, "volume": 100_000},
]


def _row(symbol, price, value):
    return {"symbol": symbol, "price": price, "value": value}


def test_computes_flow_surge_pct_and_today_value():
    board = [_row("SURGE", 24000, 210_000_000)]  # value hôm nay 210tr = 10× nền

    result = flow_surge_service.compute_flow_surge(
        board, {"SURGE": HIST}, now=NOW, min_base_vnd=0, min_price_vnd=0
    )

    row = result["rows"][0]
    assert row["gia_hien_tai"] == 24.0
    assert row["gia_tri_khop_lenh"] == 0.21           # 210tr / 1e9 (RIÊNG hôm nay)
    assert row["pct_tang"] == 900.0                   # (210tr-21tr)/21tr*100


def test_avg_window_limits_history():
    board = [_row("A", 24000, 100_000_000)]
    hist3 = [
        {"time": _epoch("2026-07-18"), "high": 5, "low": 5, "close": 5.0, "volume": 1000},
        {"time": _epoch("2026-07-20"), "high": 20, "low": 20, "close": 20.0, "volume": 1000},
        {"time": _epoch("2026-07-21"), "high": 20, "low": 20, "close": 20.0, "volume": 1000},
    ]
    result = flow_surge_service.compute_flow_surge(
        board, {"A": hist3}, now=NOW, avg_window=2, min_base_vnd=0, min_price_vnd=0
    )
    # trung bình chỉ 2 phiên gần nhất = 20tr → (100tr-20tr)/20tr*100 = 400%
    assert result["rows"][0]["pct_tang"] == 400.0


def test_sorts_desc_top_n_and_skips_no_history_or_zero_avg():
    board = [
        _row("BIG", 24000, 210_000_000),
        _row("SMALL", 24000, 42_000_000),
        _row("NOHIST", 24000, 1e8),        # thiếu history → loại
        _row("ZEROAVG", 24000, 1e8),       # nền volume 0 → avg 0 → loại
    ]
    zero = [{"time": _epoch("2026-07-21"), "high": 20, "low": 20, "close": 20.0, "volume": 0}]
    history = {"BIG": HIST, "SMALL": HIST, "ZEROAVG": zero}

    result = flow_surge_service.compute_flow_surge(
        board, history, now=NOW, top_n=1, min_base_vnd=0, min_price_vnd=0
    )

    assert [r["symbol"] for r in result["rows"]] == ["BIG"]


# ----- Lọc penny -----

def test_excludes_penny_with_low_baseline():
    """Mã nền thanh khoản nhỏ (TB value < MIN_BASE) bị loại DÙ % bắn rất cao —
    diệt penny 'chết sống dậy'."""
    penny = [
        {"time": _epoch("2026-07-20"), "high": 20, "low": 20, "close": 20.0, "volume": 10},  # value 200k
        {"time": _epoch("2026-07-21"), "high": 20, "low": 20, "close": 20.0, "volume": 10},
    ]
    board = [_row("PENNY", 20000, 500_000_000)]  # giá 20k (qua sàn giá), nhưng nền ~200k

    result = flow_surge_service.compute_flow_surge(
        board, {"PENNY": penny}, now=NOW  # dùng ngưỡng mặc định (MIN_BASE = 1 tỷ)
    )

    assert result["rows"] == []


def test_excludes_low_price_penny():
    """Mã giá bèo (< MIN_PRICE) bị loại dù nền lớn."""
    board = [_row("CHEAP", 3000, 5_000_000_000)]  # giá 3.000đ < 5.000đ

    result = flow_surge_service.compute_flow_surge(
        board, {"CHEAP": LIQUID_HIST}, now=NOW
    )

    assert result["rows"] == []


def test_includes_liquid_symbol_passing_filters():
    """Mã nền ≥ MIN_BASE và giá ≥ MIN_PRICE được giữ."""
    board = [_row("GOOD", 20000, 6_000_000_000)]  # nền 2 tỷ, giá 20k, hôm nay 6 tỷ

    result = flow_surge_service.compute_flow_surge(
        board, {"GOOD": LIQUID_HIST}, now=NOW
    )

    assert [r["symbol"] for r in result["rows"]] == ["GOOD"]
    # (6 tỷ − 2 tỷ)/2 tỷ × 100 = 200%
    assert result["rows"][0]["pct_tang"] == 200.0


def test_get_flow_surge_prefers_full_board_snapshot(monkeypatch):
    """get_flow_surge đọc board TOÀN thị trường (market_board_full) để mở rộng
    universe, fallback vn100 view nếu chưa có."""
    market_cache.reset()
    market_cache.set_snapshot(
        "market_board_full", [{"symbol": "GOOD", "price": 20000, "value": 6_000_000_000}]
    )
    monkeypatch.setattr(signal_service, "_history_candles", {"GOOD": LIQUID_HIST})
    monkeypatch.setattr(signal_service, "_cache", {"last_refresh": "2026-07-21"})

    result = flow_surge_service.get_flow_surge()

    assert [r["symbol"] for r in result["rows"]] == ["GOOD"]
    market_cache.reset()
