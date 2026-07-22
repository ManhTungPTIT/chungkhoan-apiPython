"""Chart "CHỈ SỐ CHUNG 3 SÀN" (cột nhóm) — 4 nhóm VN INDEX / HN INDEX / UP INDEX
/ VN30, mỗi nhóm 3 cột: giá trị khớp lệnh (nghìn tỷ), điểm tăng giảm, % tăng giảm.

Điểm chỉ số + đóng cửa phiên trước: lấy THẬT từ index history (close nến hôm nay
và nến áp chót). Giá trị khớp lệnh mỗi sàn: cộng `value` THẬT của cổ phiếu theo
sàn (từ market_board_full + bản đồ sàn), VN30 cộng theo danh sách thành viên —
số liệu thật, chỉ gộp, không ước lượng.
"""

import time
from datetime import datetime, timedelta

from app.data import data_source

VN_TZ = data_source.VN_TZ

# (symbol history, tên hiển thị, khóa nhóm value)
INDICES = [
    {"symbol": "VNINDEX", "name": "VN INDEX", "group": "HOSE"},
    {"symbol": "HNXINDEX", "name": "HN INDEX", "group": "HNX"},
    {"symbol": "UPCOMINDEX", "name": "UP INDEX", "group": "UPCOM"},
    {"symbol": "VN30", "name": "VN30", "group": "VN30"},
]
_EXCHANGE_GROUPS = ("HOSE", "HNX", "UPCOM")
_NGHIN_TY = 1_000_000_000_000  # 1 nghìn tỷ = 1e12 VND

# Điểm chỉ số đổi chậm — memoize theo TTL để FE poll không bắt fetch mỗi lần.
_POINTS_TTL_S = 60
_points_cache = {"at": 0.0, "data": {}}


def exchange_values_from_board(board_rows, exchange_map, vn30_members=None):
    """Tổng value (VND) theo sàn (chuẩn hoá HSX→HOSE) + VN30 theo member list."""
    totals = {g: 0 for g in _EXCHANGE_GROUPS}
    totals["VN30"] = 0
    vn30 = set(vn30_members or [])
    for row in board_rows or []:
        symbol = row.get("symbol")
        value = row.get("value") or 0
        exch = data_source._normalize_exchange(exchange_map.get(symbol) if exchange_map else None)
        if exch in totals:
            totals[exch] += value
        if symbol in vn30:
            totals["VN30"] += value
    return totals


def compute_index_overview(index_points, exchange_values):
    """Hàm thuần: index_points {symbol: {current, prev}} + exchange_values
    {group: value_vnd} → payload chart. Index thiếu điểm vẫn ra row (điểm/%=None)."""
    rows = []
    for cfg in INDICES:
        pts = (index_points or {}).get(cfg["symbol"]) or {}
        current = pts.get("current")
        prev = pts.get("prev")
        change = round(current - prev, 2) if current is not None and prev is not None else None
        pct = (
            round((current - prev) / prev * 100, 2)
            if current is not None and prev
            else None
        )
        value_vnd = (exchange_values or {}).get(cfg["group"]) or 0
        rows.append(
            {
                "ten_san": cfg["name"],
                "diem_hien_tai": round(current, 2) if current is not None else None,
                "diem_dong_cua_phien_truoc": round(prev, 2) if prev is not None else None,
                "gia_tri_khop_lenh": round(value_vnd / _NGHIN_TY, 2),  # nghìn tỷ
                "diem_tang_giam": change,
                "pct": pct,
            }
        )
    return {"indices": rows}


def _fetch_index_points(history_fn=None, now=None):
    """Điểm hiện tại + đóng cửa phiên trước cho từng index từ nến 1D (fetch ~15
    ngày, lấy 2 nến cuối). Index nào lỗi/thiếu (vd VN30 nếu nguồn không hỗ trợ) →
    bỏ qua, chart vẫn hiện các sàn còn lại. Trả {symbol: {current, prev}}."""
    history_fn = history_fn or data_source.fetch_intraday_history
    now = now or datetime.now(VN_TZ)
    end = now.strftime("%Y-%m-%d")
    start = (now - timedelta(days=15)).strftime("%Y-%m-%d")
    out = {}
    for cfg in INDICES:
        try:
            rows = history_fn(cfg["symbol"], start, end, "1D")
        except BaseException:  # noqa: BLE001 — index lỗi không chặn chart
            rows = None
        if not rows:
            continue
        try:
            current = float(rows[-1]["close"])
        except (KeyError, TypeError, ValueError, IndexError):
            continue
        prev = None
        if len(rows) >= 2:
            try:
                prev = float(rows[-2]["close"])
            except (KeyError, TypeError, ValueError):
                prev = None
        out[cfg["symbol"]] = {"current": current, "prev": prev}
    return out


def _index_points_cached(history_fn=None, now=None):
    """Điểm chỉ số memoize theo TTL (last-good khi fetch mới rỗng)."""
    now_mono = time.monotonic()
    if _points_cache["data"] and now_mono - _points_cache["at"] < _POINTS_TTL_S:
        return _points_cache["data"]
    fetched = _fetch_index_points(history_fn, now)
    if fetched:
        _points_cache["data"] = fetched
        _points_cache["at"] = now_mono
    return _points_cache["data"]


def get_index_overview():
    """Điểm gọi runtime: value/sàn từ market_board_full (cache) + bản đồ sàn +
    VN30 members; điểm chỉ số fetch (memoize TTL). Không phụ thuộc luồng refresh."""
    from app.data import market_cache
    from app.services import vn100_service

    board_rows = market_cache.get_snapshot("market_board_full") or []
    exchange_map = data_source._symbol_exchange_map()
    vn30_members = vn100_service.get_vn30_members()
    exchange_values = exchange_values_from_board(board_rows, exchange_map, vn30_members)

    index_points = _index_points_cached()
    result = compute_index_overview(index_points, exchange_values)
    result["generated_at"] = datetime.now(VN_TZ).isoformat()
    return result
