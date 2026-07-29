"""Chart "DÒNG TIỀN TĂNG ĐỘT BIẾN NỔI BẬT HÔM NAY" — đo mức đột biến thanh khoản
trong 1 phiên (KHÔNG phải biến động giá).

Khác biệt cốt lõi: cột xanh = % tăng DÒNG TIỀN = (value hôm nay − NỀN) / NỀN × 100
→ mã hôm nay giao dịch gấp nhiều lần bình thường thì % này bắn rất cao. Cột tím =
value RIÊNG hôm nay (không cộng dồn).

NỀN = TB value N phiên gần nhất × tỉ lệ theo KHUNG GIỜ (xem `BASELINE_CURVE`).

Đọc cùng cache RAM (market_wide/board_vn100 + _history_candles), không gọi vnstock.
value phiên nền = volume × close × 1000 (close nghìn đồng → VND); value hôm nay lấy
trực tiếp từ board (VND).
"""

from datetime import datetime, time

from app.services import signal_service

DEFAULT_TOP_N = 30
DEFAULT_AVG_WINDOW = 20  # số phiên nền để tính trung bình (spec: ví dụ 10 hoặc 20)

# Nền so sánh theo KHUNG GIỜ. TB20 là dòng tiền TRỌN phiên, còn value hôm nay mới
# tích luỹ được một phần → so thẳng thì 9h30 mã nào cũng âm sâu, 14h30 lại dương
# ảo. Mỗi khung giờ chỉ so với phần TB20 mà một phiên bình thường đã khớp được
# tới thời điểm đó:
#   09:00–10:00 → 20% TB20 | 10:00–11:30 → 45% | 13:00–14:00 → 70% | 14:00–15:00 → 100%
# Mốc trong bảng là ĐẦU khung và có hiệu lực tới mốc kế tiếp, nên 3 quãng bảng
# không liệt kê được phủ luôn:
#   - trước 09:00 (gồm ATO) giữ 20% → mẫu số không bao giờ bằng 0;
#   - nghỉ trưa 11:30–13:00 giữ 45% vì tiền đứng yên từ 11:30, đổi nền sẽ làm %
#     nhảy dù không có lệnh nào khớp;
#   - sau 15:00 giữ 100% — phiên đã đóng, so trọn phiên mới đúng.
BASELINE_CURVE = (
    (time(0, 0), "09:00–10:00", 0.20),
    (time(10, 0), "10:00–11:30", 0.45),
    (time(13, 0), "13:00–14:00", 0.70),
    (time(14, 0), "14:00–15:00", 1.00),
)


def baseline_bucket(now):
    """(nhãn khung giờ, tỉ lệ TB20 dùng làm nền) tại thời điểm `now` (giờ VN).

    Lấy khung CUỐI CÙNG có mốc bắt đầu ≤ `now` — biên là nửa mở, đúng 10:00 đã
    thuộc khung 10:00–11:30."""
    current = now.time()
    label, factor = BASELINE_CURVE[0][1], BASELINE_CURVE[0][2]
    for start, bucket_label, bucket_factor in BASELINE_CURVE:
        if current < start:
            break
        label, factor = bucket_label, bucket_factor
    return label, factor


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
    min_price_vnd (giá). Trả {rows, avg_window, baseline_factor, time_bucket}."""
    now = now or datetime.now(signal_service.VN_TZ)
    scale = signal_service.PRICE_BOARD_SCALE
    time_bucket, factor = baseline_bucket(now)

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
        # ảo cao dù tiền tuyệt đối không đáng kể). Chấm trên TB TRỌN phiên chứ
        # không phải nền đã scale: đây là bộ lọc về thanh khoản vốn có của mã,
        # không được nới lỏng dần theo giờ trong ngày.
        if avg_vnd < min_base_vnd:
            continue
        base_vnd = avg_vnd * factor   # > 0 vì factor nhỏ nhất là 0.20

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
        pct = round((today_vnd - base_vnd) / base_vnd * 100, 2)

        rows.append(
            {
                "symbol": symbol,
                "gia_tri_khop_lenh": round(today_vnd / _VND_TO_TY, 3),  # RIÊNG hôm nay
                "gia_hien_tai": round(gia_hien_tai, 2),
                "pct_tang": pct,
            }
        )

    rows.sort(key=lambda r: r["pct_tang"], reverse=True)
    return {
        "rows": rows[:top_n],
        "avg_window": avg_window,
        "time_bucket": time_bucket,
        "baseline_factor": factor,
    }


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
