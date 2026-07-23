"""Chart "DÒNG TIỀN TĂNG ĐỘT BIẾN NỔI BẬT HÔM NAY" — đo mức đột biến thanh khoản
trong 1 phiên (KHÔNG phải biến động giá).

Khác biệt cốt lõi: cột xanh = % tăng DÒNG TIỀN = (value hôm nay − trung bình value
N phiên gần nhất) / trung bình × 100 → mã hôm nay giao dịch gấp nhiều lần bình
thường thì % này bắn rất cao. Cột tím = value RIÊNG hôm nay (không cộng dồn).

Đọc cùng cache RAM (market_wide/board_vn100 + _history_candles), không gọi vnstock.
value phiên nền = volume × close × 1000 (close nghìn đồng → VND); value hôm nay lấy
trực tiếp từ board (VND).
"""

from datetime import datetime

from app.services import signal_service

DEFAULT_TOP_N = 30
DEFAULT_AVG_WINDOW = 20  # số phiên nền để tính trung bình (spec: ví dụ 10 hoặc 20)

# Lọc chất lượng để bỏ penny tăng đột biến:
#  - MIN_BASELINE_VND: TB value NỀN tối thiểu → mã phải vốn dĩ có dòng tiền thật;
#    "đột biến" là tiền nhân lên, không phải penny thanh khoản ~0 vừa spike.
#  - MIN_PRICE_VND: chặn cổ phiếu giá bèo theo định nghĩa penny.
MIN_BASELINE_VND = 1_000_000_000  # 1 tỷ
MIN_PRICE_VND = 5_000             # 5.000đ

_NGHIN_TO_VND = 1000
_VND_TO_TY = 1_000_000_000


def _avg_history_value_vnd(candles, avg_window):
    """Trung bình value (VND) của tối đa `avg_window` phiên gần nhất trong history.
    Bỏ nến thiếu volume/close. None nếu không có phiên hợp lệ."""
    values = []
    for candle in candles[-avg_window:] if avg_window > 0 else []:
        volume = candle.get("volume")
        close = candle.get("close")
        if volume and close:
            values.append(volume * close * _NGHIN_TO_VND)
    if not values:
        return None
    return sum(values) / len(values)


def compute_flow_surge(
    board_rows,
    history_candles,
    now=None,
    top_n=DEFAULT_TOP_N,
    avg_window=DEFAULT_AVG_WINDOW,
    min_base_vnd=MIN_BASELINE_VND,
    min_price_vnd=MIN_PRICE_VND,
):
    """Hàm thuần: board_rows (symbol/price/value hôm nay) + history_candles
    ({mã: [nến ngày có volume]}). Lọc penny bằng min_base_vnd (TB nền) và
    min_price_vnd (giá). Trả {rows, avg_window}."""
    now = now or datetime.now(signal_service.VN_TZ)
    scale = signal_service.PRICE_BOARD_SCALE

    rows = []
    for row in board_rows or []:
        symbol = row.get("symbol")
        candles = (history_candles or {}).get(symbol)
        if not candles:
            continue
        avg_vnd = _avg_history_value_vnd(candles, avg_window)
        if not avg_vnd or avg_vnd <= 0:
            continue
        # Sàn thanh khoản NỀN — loại penny có TB dòng tiền quá nhỏ (tỷ lệ % bắn
        # ảo cao dù tiền tuyệt đối không đáng kể).
        if avg_vnd < min_base_vnd:
            continue

        price = row.get("price") or 0
        gia_hien_tai = price / scale if price > 0 else candles[-1].get("close", 0)
        if not gia_hien_tai or gia_hien_tai <= 0:
            continue
        # Sàn giá — loại cổ phiếu giá bèo (penny theo giá).
        if gia_hien_tai * scale < min_price_vnd:
            continue

        today_vnd = row.get("value") or 0
        if today_vnd <= 0:
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


def get_flow_surge(top_n=DEFAULT_TOP_N, avg_window=DEFAULT_AVG_WINDOW):
    """Điểm gọi runtime: đọc board (market_wide→board_vn100) + nến base RAM."""
    from app.data import market_cache

    # Ưu tiên board TOÀN thị trường (market_board_full) để mở rộng universe
    # (không bị chặn value>1 tỷ như view vn100); fallback view vn100 nếu chưa có.
    board_rows = market_cache.get_snapshot("market_board_full")
    if not board_rows:
        wide = market_cache.get_snapshot("market_wide") or {}
        snap = market_cache.get_snapshot("board_vn100") or {}
        vn100_view = wide.get("vn100") or snap.get("vn100") or {}
        board_rows = vn100_view.get("data") or []
    history = getattr(signal_service, "_history_candles", None) or {}

    result = compute_flow_surge(board_rows, history, top_n=top_n, avg_window=avg_window)
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
