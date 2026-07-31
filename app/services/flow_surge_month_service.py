"""Chart "DÒNG TIỀN TĂNG ĐỘT BIẾN SO VỚI BÌNH QUÂN 1 THÁNG".

20 phiên nền ≈ 1 tháng: tháng dương lịch tính cả cuối tuần và lễ thì chỉ còn
khoảng 20 phiên giao dịch.

Xếp hạng theo ĐIỂM:

    diem = % đột biến thanh khoản × log10(thanh khoản Tỷ + 1) × (1 + % tăng giá)

  - % đột biến = (value hôm nay − NỀN cùng khung giờ) / NỀN × 100 (cột xanh);
  - log10 theo TỶ đồng, không phải VND — cùng khuôn ba chart điểm còn lại;
  - `% tăng giá` vào dưới dạng THẬP PHÂN: mã +7% → nhân 1,07 (chốt với người dùng
    31/07). Biên độ trần/sàn ±7…15% nên hệ số nằm trong 0,85–1,15 — giá chỉ
    nghiêng nhẹ thứ hạng, dòng tiền vẫn quyết định, đúng tên chart. Đưa vào dạng
    số phần trăm (1 + 7 = 8) thì giá áp đảo và mã giảm quá 1% cho hệ số ÂM, khiến
    đột biến càng mạnh càng tụt sâu.

NỀN dùng chung `flow_surge_service.baseline_bucket` — TB20 là dòng tiền TRỌN
phiên, còn value hôm nay mới tích luỹ một phần, so thẳng thì sáng sớm mã nào cũng
âm sâu. (31/07 bổ sung; trước đó chart này so thẳng với TB20 trọn phiên.)

Biến thể CHẶT hơn của flow_surge (chart "hôm nay") ở 2 bộ lọc thêm theo spec:
  - session_count >= 20  → loại mã mới niêm yết / mới giao dịch lại (chưa đủ nền).
  - match_value_T >= 5e9 → loại mã đột biến % cao nhưng tiền tuyệt đối bé.
Giữ nguyên: nền >= 1 tỷ, giá >= 5.000đ.

Tách RIÊNG khỏi flow_surge_service để KHÔNG đụng chart "hôm nay" — chỉ mượn lại
bảng khung giờ. Cùng nguồn cache RAM (market_board_full + _history_candles),
không gọi vnstock. value phiên nền = volume × close × 1000 (close nghìn đồng →
VND, đúng nghĩa khớp lệnh, không gồm thỏa thuận); value hôm nay và `change_pct`
lấy trực tiếp từ board.
"""

import math
from datetime import datetime

from app.services import flow_surge_service, signal_service

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
    """Hàm thuần: board_rows (symbol/price/value/change_pct hôm nay) +
    history_candles ({mã: [nến ngày có volume]}) →
    {rows, avg_window, time_bucket, baseline_factor}. Các ngưỡng là tham số để
    test toán học cô lập được từng bộ lọc."""
    now = now or datetime.now(signal_service.VN_TZ)
    scale = signal_service.PRICE_BOARD_SCALE
    time_bucket, factor = flow_surge_service.baseline_bucket(now)

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
        # Nền thanh khoản — loại penny có TB dòng tiền quá nhỏ (% bắn ảo). Chấm
        # trên TB TRỌN phiên chứ không phải nền đã scale: đây là bộ lọc về thanh
        # khoản vốn có của mã, không được nới lỏng dần theo giờ trong ngày.
        if avg_vnd < min_base_vnd:
            continue
        base_vnd = avg_vnd * factor   # > 0 vì factor nhỏ nhất là 0.10

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
        pct = round((today_vnd - base_vnd) / base_vnd * 100, 2)

        # % tăng GIÁ hôm nay (so giá tham chiếu) — board đã tính sẵn ở
        # data_source._map_board, không phải dựng lại từ nến. Thiếu cột → 0, tức
        # hệ số 1.0: mã đó xếp hạng thuần theo dòng tiền thay vì bị loại.
        pct_gia = row.get("change_pct") or 0
        gia_tri_ty = today_vnd / _VND_TO_TY
        diem = pct * math.log10(gia_tri_ty + 1) * (1 + pct_gia / 100)

        rows.append(
            {
                "symbol": symbol,
                "gia_tri_khop_lenh": round(gia_tri_ty, 3),  # RIÊNG hôm nay
                "gia_hien_tai": round(gia_hien_tai, 2),
                "pct_tang": pct,
                "pct_gia": round(pct_gia, 2),
                "diem": round(diem, 2),
            }
        )

    rows.sort(key=lambda r: r["diem"], reverse=True)
    return {
        "rows": rows[:top_n],
        "avg_window": avg_window,
        "time_bucket": time_bucket,
        "baseline_factor": factor,
    }


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
