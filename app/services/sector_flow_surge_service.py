# app/services/sector_flow_surge_service.py
"""Chart 'NGÀNH CÓ DÒNG TIỀN TĂNG ĐỘT BIẾN' — bản gộp-theo-ngành của
flow_surge_service. Dòng tiền đột biến = value hôm nay so với TB N phiên nền,
cộng dồn theo icb_code (ICB cấp 3) thay vì từng mã.

Xếp hạng theo ĐIỂM NGÀNH:

    điểm = % đột biến
           × log10(tổng thanh khoản ngành Tỷ + 1)
           × (số mã tăng / (số mã tăng + số mã giảm))

    % đột biến = (thanh khoản hiện tại / NỀN cùng thời điểm − 1) × 100

Ba thừa số trả lời ba câu khác nhau, thiếu cái nào cũng lệch: mức đột biến nói
tiền vào GẤP mấy lần thường lệ, log10 chặn ngành bé xíu spike vài lần vẫn đứng
trên ngành nghìn tỷ, còn tỷ lệ mã tăng phân biệt "tiền vào để mua" với "tiền vào
để tháo chạy" — cùng +200% dòng tiền, ngành xanh 80% mã ăn điểm gấp 4 lần ngành
chỉ 20% mã xanh. log10 tính theo TỶ như các chart khác (theo VND mọi ngành rơi
vào 11…13, trọng số triệt tiêu).

NỀN = TB value `avg_window` phiên × tỉ lệ theo KHUNG GIỜ, dùng lại
`flow_surge_service.BASELINE_CURVE` (09:30 → 10% … 15:00 → 100%) để chart ngành
và chart mã không bao giờ lệch định nghĩa. Không scale theo giờ thì 9h30 ngành
nào cũng "hụt dòng tiền" −80% vì TB20 là con số TRỌN phiên.

Đọc cùng cache RAM (market_board_full/market_wide/board_vn100 +
_history_candles) + bản đồ ngành memoize trong sector_service — KHÔNG gọi
vnstock theo request.
"""

import math
from datetime import datetime

from app.services import flow_surge_service, sector_service, signal_service

DEFAULT_AVG_WINDOW = 20
_VND_TO_TY = 1_000_000_000

# Cùng ngưỡng lọc penny với flow_surge_service để 2 chart nhất quán.
MIN_BASELINE_VND = flow_surge_service.MIN_BASELINE_VND


def compute_sector_flow_surge(
    board_rows,
    history_candles,
    industry_map,
    now=None,
    avg_window=DEFAULT_AVG_WINDOW,
    min_base_vnd=MIN_BASELINE_VND,
):
    """Hàm thuần — dễ test giống compute_flow_surge. Cộng dồn value hôm nay
    và value TB nền của từng mã vào đúng icb_code, rồi tính % đột biến trên
    tổng đã cộng dồn (không phải trung bình % từng mã — khớp cách chart 1
    hiển thị: % của CẢ NGÀNH, không phải % trung bình các mã)."""
    now = now or datetime.now(signal_service.VN_TZ)
    time_bucket, factor = flow_surge_service.baseline_bucket(now)
    agg: dict[str, dict] = {}

    for row in board_rows or []:
        symbol = row.get("symbol")
        if not symbol:
            continue

        info = (industry_map or {}).get(symbol) or {}
        icb_code = info.get("icb_code", "")
        group_name = info.get("icb_name", sector_service.UNCLASSIFIED)

        bucket = agg.setdefault(
            icb_code,
            {
                "group": group_name,
                "value_today": 0.0,
                "avg_value": 0.0,
                "so_ma_tang": 0,
                "so_ma_giam": 0,
            },
        )

        # Độ rộng đếm trên MỌI mã của ngành có mặt trên board, kể cả mã chưa đủ
        # lịch sử để vào phần dòng tiền: "số mã tăng/giảm" là trạng thái giá của
        # cả ngành, không phải của riêng nhóm mã tính được nền.
        change_pct = row.get("change_pct") or 0
        if change_pct > 0:
            bucket["so_ma_tang"] += 1
        elif change_pct < 0:
            bucket["so_ma_giam"] += 1

        candles = (history_candles or {}).get(symbol)
        if not candles:
            continue
        avg_vnd = flow_surge_service._avg_history_value_vnd(candles, avg_window)
        if not avg_vnd or avg_vnd <= 0:
            continue

        bucket["value_today"] += row.get("value") or 0
        bucket["avg_value"] += avg_vnd

    result = []
    for icb_code, v in agg.items():
        avg_v = v["avg_value"]
        # Lọc ngành có TB nền quá nhỏ (ít mã đủ chuẩn / ngành toàn penny). Chấm
        # trên TB TRỌN phiên chứ không phải nền đã scale — đây là bộ lọc về quy
        # mô vốn có của ngành, không được nới lỏng dần theo giờ trong ngày.
        if avg_v < min_base_vnd:
            continue
        base_v = avg_v * factor      # > 0 vì factor nhỏ nhất là 0.10
        pct = (v["value_today"] - base_v) / base_v * 100
        gia_tri_ty = v["value_today"] / _VND_TO_TY

        movers = v["so_ma_tang"] + v["so_ma_giam"]
        # Cả ngành đứng giá (hoặc board chưa khớp) → không có tín hiệu hướng đi,
        # cho 0 thay vì 0/0. Ngành đó tụt về cuối bảng, đúng ý "chưa có gì".
        breadth = v["so_ma_tang"] / movers if movers else 0.0
        diem = pct * math.log10(gia_tri_ty + 1) * breadth

        result.append(
            {
                "group": v["group"],
                "icb_code": icb_code,
                "gia_tri_khop_lenh": round(gia_tri_ty, 2),          # cột tím, tỷ
                "duong_trung_binh": round(avg_v / _VND_TO_TY, 2),   # chấm vàng, tỷ (TB TRỌN phiên)
                "pct_tang": round(pct, 2),                          # thanh xanh/đỏ
                "so_ma_tang": v["so_ma_tang"],
                "so_ma_giam": v["so_ma_giam"],
                "ty_le_ma_tang": round(breadth, 4),
                "diem": round(diem, 2),
            }
        )

    result.sort(key=lambda r: r["diem"], reverse=True)
    return {
        "rows": result,
        "avg_window": avg_window,
        "time_bucket": time_bucket,
        "baseline_factor": factor,
    }


def get_sector_flow_surge(avg_window=DEFAULT_AVG_WINDOW):
    """Điểm gọi runtime — cùng nguồn board với get_flow_surge()."""
    from app.data import market_cache

    board_rows = market_cache.get_snapshot("market_board_full")
    if not board_rows:
        wide = market_cache.get_snapshot("market_wide") or {}
        snap = market_cache.get_snapshot("board_vn100") or {}
        vn100_view = wide.get("vn100") or snap.get("vn100") or {}
        board_rows = vn100_view.get("data") or []

    history = getattr(signal_service, "_history_candles", None) or {}
    industry_map = sector_service.get_industry_map()

    result = compute_sector_flow_surge(board_rows, history, industry_map, avg_window=avg_window)
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
