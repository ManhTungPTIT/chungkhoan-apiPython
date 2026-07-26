# app/services/sector_flow_surge_service.py
"""Chart 'NGÀNH CÓ DÒNG TIỀN TĂNG ĐỘT BIẾN' — bản gộp-theo-ngành của
flow_surge_service. Dòng tiền đột biến = value hôm nay so với TB N phiên nền,
cộng dồn theo icb_code (ICB cấp 3) thay vì từng mã.

Đọc cùng cache RAM (market_board_full/market_wide/board_vn100 +
_history_candles) + bản đồ ngành memoize trong sector_service — KHÔNG gọi
vnstock theo request.
"""

from app.services import flow_surge_service, sector_service

DEFAULT_AVG_WINDOW = 20
_VND_TO_TY = 1_000_000_000

# Cùng ngưỡng lọc penny với flow_surge_service để 2 chart nhất quán.
MIN_BASELINE_VND = flow_surge_service.MIN_BASELINE_VND


def compute_sector_flow_surge(
    board_rows,
    history_candles,
    industry_map,
    avg_window=DEFAULT_AVG_WINDOW,
    min_base_vnd=MIN_BASELINE_VND,
):
    """Hàm thuần — dễ test giống compute_flow_surge. Cộng dồn value hôm nay
    và value TB nền của từng mã vào đúng icb_code, rồi tính % đột biến trên
    tổng đã cộng dồn (không phải trung bình % từng mã — khớp cách chart 1
    hiển thị: % của CẢ NGÀNH, không phải % trung bình các mã)."""
    agg: dict[str, dict] = {}

    for row in board_rows or []:
        symbol = row.get("symbol")
        candles = (history_candles or {}).get(symbol)
        if not candles:
            continue
        avg_vnd = flow_surge_service._avg_history_value_vnd(candles, avg_window)
        if not avg_vnd or avg_vnd <= 0:
            continue

        info = industry_map.get(symbol) or {}
        icb_code = info.get("icb_code", "")
        group_name = info.get("icb_name", sector_service.UNCLASSIFIED)

        bucket = agg.setdefault(
            icb_code, {"group": group_name, "value_today": 0.0, "avg_value": 0.0}
        )
        bucket["value_today"] += row.get("value") or 0
        bucket["avg_value"] += avg_vnd

    result = []
    for icb_code, v in agg.items():
        avg_v = v["avg_value"]
        # Lọc ngành có TB nền quá nhỏ (ít mã đủ chuẩn / ngành toàn penny).
        if avg_v < min_base_vnd:
            continue
        pct = round((v["value_today"] - avg_v) / avg_v * 100, 2) if avg_v else 0.0
        result.append(
            {
                "group": v["group"],
                "icb_code": icb_code,
                "gia_tri_khop_lenh": round(v["value_today"] / _VND_TO_TY, 2),  # cột tím, tỷ
                "duong_trung_binh": round(avg_v / _VND_TO_TY, 2),              # chấm vàng, tỷ
                "pct_tang": pct,                                                # thanh xanh/đỏ
            }
        )

    result.sort(key=lambda r: r["pct_tang"], reverse=True)
    return {"rows": result, "avg_window": avg_window}


def get_sector_flow_surge(avg_window=DEFAULT_AVG_WINDOW):
    """Điểm gọi runtime — cùng nguồn board với get_flow_surge()."""
    from app.data import market_cache
    from app.services import signal_service

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