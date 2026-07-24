"""Chart "DÒNG TIỀN TĂNG ĐỘT BIẾN SO VỚI BÌNH QUÂN 1 THÁNG".

Biến thể CHẶT hơn của flow_surge (chart "hôm nay"): cùng công thức
  % = (value hôm nay − TB value 20 phiên nền) / TB × 100
nhưng thêm 2 bộ lọc theo spec để bảng xếp hạng "sạch" hơn:
  - session_count >= 20  → loại mã mới niêm yết / mới giao dịch lại (chưa đủ nền).
  - match_value_T >= 5e9 → loại mã đột biến % cao nhưng tiền tuyệt đối bé.
Giữ nguyên: nền >= 1 tỷ, giá >= 5.000đ.

Tách RIÊNG khỏi flow_surge_service để KHÔNG đụng chart "hôm nay". Cùng nguồn cache
RAM (market_board_full + _history_candles), không gọi vnstock. value phiên nền =
volume × close × 1000 (close nghìn đồng → VND, đúng nghĩa khớp lệnh, không gồm
thỏa thuận); value hôm nay lấy trực tiếp từ board (VND).
"""

from datetime import datetime

from app.services import signal_service

DEFAULT_TOP_N = 20
DEFAULT_AVG_WINDOW = 20  # số phiên nền tính trung bình (≈ 1 tháng giao dịch)

# Bộ lọc chất lượng (spec "3. Xử lý cho biểu đồ DÒNG TIỀN TĂNG ĐỘT BIẾN"):
MIN_SESSIONS = 20                  # session_count >= 20
MIN_BASELINE_VND = 1_000_000_000   # avg_20 >= 1e9  (nền thanh khoản thật)
MIN_TODAY_VND = 5_000_000_000      # match_value_T >= 5e9 (tiền hôm nay đáng kể)
MIN_PRICE_VND = 5_000             # loại cổ phiếu giá bèo (penny theo giá)

_NGHIN_TO_VND = 1000
_VND_TO_TY = 1_000_000_000


def _baseline_value_vnd(candles, avg_window):
    """Trả (TB value VND, số phiên hợp lệ) của tối đa `avg_window` phiên gần nhất.
    Bỏ nến thiếu volume/close. (None, 0) nếu không có phiên hợp lệ — số phiên dùng
    để loại mã chưa đủ nền (mới niêm yết)."""
    values = []
    for candle in candles[-avg_window:] if avg_window > 0 else []:
        volume = candle.get("volume")
        close = candle.get("close")
        if volume and close:
            values.append(volume * close * _NGHIN_TO_VND)
    if not values:
        return None, 0
    return sum(values) / len(values), len(values)


def compute_flow_surge_month(
    board_rows,
    history_candles,
    now=None,
    top_n=DEFAULT_TOP_N,
    avg_window=DEFAULT_AVG_WINDOW,
    min_sessions=MIN_SESSIONS,
    min_base_vnd=MIN_BASELINE_VND,
    min_today_vnd=MIN_TODAY_VND,
    min_price_vnd=MIN_PRICE_VND,
):
    """Hàm thuần: board_rows (symbol/price/value hôm nay) + history_candles
    ({mã: [nến ngày có volume]}) → {rows, avg_window}. Các ngưỡng là tham số để
    test toán học cô lập được từng bộ lọc."""
    now = now or datetime.now(signal_service.VN_TZ)
    scale = signal_service.PRICE_BOARD_SCALE

    rows = []
    for row in board_rows or []:
        symbol = row.get("symbol")
        candles = (history_candles or {}).get(symbol)
        if not candles:
            continue
        avg_vnd, session_count = _baseline_value_vnd(candles, avg_window)
        if not avg_vnd or avg_vnd <= 0:
            continue
        # Đủ phiên nền — loại mã mới niêm yết / mới giao dịch lại.
        if session_count < min_sessions:
            continue
        # Nền thanh khoản — loại penny có TB dòng tiền quá nhỏ (% bắn ảo).
        if avg_vnd < min_base_vnd:
            continue

        price = row.get("price") or 0
        gia_hien_tai = price / scale if price > 0 else candles[-1].get("close", 0)
        if not gia_hien_tai or gia_hien_tai <= 0:
            continue
        # Sàn giá — loại cổ phiếu giá bèo.
        if gia_hien_tai * scale < min_price_vnd:
            continue

        today_vnd = row.get("value") or 0
        if today_vnd <= 0:
            continue
        # Sàn tiền HÔM NAY — bỏ mã đột biến % cao nhưng tiền tuyệt đối bé.
        if today_vnd < min_today_vnd:
            continue
        pct = round((today_vnd - avg_vnd) / avg_vnd * 100, 2)

        rows.append(
            {
                "symbol": symbol,
                "gia_tri_khop_lenh": round(today_vnd / _VND_TO_TY, 3),  # RIÊNG hôm nay
                "gia_hien_tai": round(gia_hien_tai, 2),
                "pct_tang": pct,
            }
        )

    rows.sort(key=lambda r: r["pct_tang"], reverse=True)
    return {"rows": rows[:top_n], "avg_window": avg_window}


def get_flow_surge_month(top_n=DEFAULT_TOP_N, avg_window=DEFAULT_AVG_WINDOW):
    """Điểm gọi runtime: đọc board (market_board_full → vn100) + nến base RAM."""
    from app.data import market_cache

    board_rows = market_cache.get_snapshot("market_board_full")
    if not board_rows:
        wide = market_cache.get_snapshot("market_wide") or {}
        snap = market_cache.get_snapshot("board_vn100") or {}
        vn100_view = wide.get("vn100") or snap.get("vn100") or {}
        board_rows = vn100_view.get("data") or []
    history = getattr(signal_service, "_history_candles", None) or {}

    result = compute_flow_surge_month(
        board_rows, history, top_n=top_n, avg_window=avg_window
    )
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
