"""Chart "TOÀN CẢNH CHỈ SỐ" (cột nhóm) — 4 nhóm VN INDEX / HN INDEX / UP INDEX /
VN30, mỗi nhóm 3 cột: ĐỘ THANH KHOẢN (%), điểm tăng giảm, % tăng giảm.

Điểm chỉ số + đóng cửa phiên trước: lấy THẬT từ index history (close nến hôm nay
và nến áp chót). Giá trị khớp lệnh mỗi sàn: cộng `value` THẬT của cổ phiếu theo
sàn (từ market_board_full + bản đồ sàn), VN30 cộng theo danh sách thành viên —
số liệu thật, chỉ gộp, không ước lượng.

ĐỘ THANH KHOẢN = giá trị GD hôm nay / TB các phiên trước × 100. Con số tuyệt đối
(18 nghìn tỷ) không nói lên cao hay thấp; tỷ lệ so với nền thì có. 100% = đúng
bằng nền gần đây.
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
_ALL_GROUPS = _EXCHANGE_GROUPS + ("VN30",)  # VN30 là tập con của HOSE, không cộng dồn
_NGHIN_TY = 1_000_000_000_000  # 1 nghìn tỷ = 1e12 VND
_NGHIN_TO_VND = 1000  # nến history theo nghìn đồng, board theo VND thô

# Điểm chỉ số đổi chậm — memoize theo TTL để FE poll không bắt fetch mỗi lần.
_POINTS_TTL_S = 60
_points_cache = {"at": 0.0, "data": {}}


def exchange_values_from_board(board_rows, exchange_map, vn30_members=None):
    """Tổng value (VND) theo sàn (chuẩn hoá HSX→HOSE) + VN30 theo member list."""
    totals = dict.fromkeys(_ALL_GROUPS, 0)
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


def exchange_avg_values(history, exchange_map, vn30_members=None, today=None):
    """Giá trị GD trung bình mỗi nhóm qua các phiên ĐÃ ĐÓNG trong `history`.

    history: {symbol: [{time, close, volume}]} — snapshot sector_flow_history.
    Trả (avgs {group: vnd}, so_phien). ({}, 0) nếu không có phiên nào đã đóng.

    Nến history theo NGHÌN đồng (xem signal_service.PRICE_BOARD_SCALE) nên phải
    nhân _NGHIN_TO_VND — cùng cách period_gain_service/flow_surge_service quy đổi.
    Phiên hôm nay bị loại: nó đang chạy dở, gộp vào nền sẽ tự kéo mẫu số về phía
    tử số và làm tỷ lệ luôn xấp xỉ 100%.
    """
    today = str(today or datetime.now(VN_TZ).date().isoformat())[:10]
    vn30 = set(vn30_members or [])

    per_session = {}  # {ngày: {group: tổng vnd}}
    for symbol, candles in (history or {}).items():
        exch = data_source._normalize_exchange(
            exchange_map.get(symbol) if exchange_map else None
        )
        in_vn30 = symbol in vn30
        for candle in candles or []:
            day = data_source._history_time_date(candle.get("time"))
            if not day or day >= today:
                continue
            bucket = per_session.setdefault(day, dict.fromkeys(_ALL_GROUPS, 0))
            close = candle.get("close") or 0
            volume = candle.get("volume") or 0
            value = close * volume * _NGHIN_TO_VND
            if exch in bucket:
                bucket[exch] += value
            if in_vn30:
                bucket["VN30"] += value

    so_phien = len(per_session)
    if not so_phien:
        return {}, 0
    avgs = {
        g: sum(b[g] for b in per_session.values()) / so_phien for g in _ALL_GROUPS
    }
    return avgs, so_phien


def exchange_today_values(board_rows, exchange_map, vn30_members=None, universe=None):
    """Tổng `price × volume` (VND) hôm nay mỗi nhóm — mẫu số của tỷ lệ thanh khoản.

    KHÔNG dùng `value` (accumulated_value) dù nó chính xác hơn: mẫu số chỉ tính
    được bằng close × volume, trộn hai công thức làm tỷ lệ lệch hệ thống.
    `universe` (khóa của sector_flow_history) giới hạn rổ mã cho khớp mẫu số —
    board có cả chứng quyền/trái phiếu mà history thì không.
    """
    totals = dict.fromkeys(_ALL_GROUPS, 0)
    vn30 = set(vn30_members or [])
    for row in board_rows or []:
        symbol = row.get("symbol")
        if universe is not None and symbol not in universe:
            continue
        exch = data_source._normalize_exchange(
            exchange_map.get(symbol) if exchange_map else None
        )
        value = (row.get("price") or 0) * (row.get("volume") or 0)
        if exch in totals:
            totals[exch] += value
        if symbol in vn30:
            totals["VN30"] += value
    return totals


def liquidity_pct(today_values, avg_values):
    """{group: %} — hôm nay so với nền. None khi thiếu nền (mẫu số 0/vắng mặt);
    0.0 là giá trị THẬT (chưa có giao dịch), không nhập nhằng với thiếu dữ liệu."""
    out = {}
    for group in _ALL_GROUPS:
        avg = (avg_values or {}).get(group) or 0
        if avg <= 0:
            out[group] = None
            continue
        out[group] = round(((today_values or {}).get(group) or 0) / avg * 100, 2)
    return out


def compute_index_overview(index_points, exchange_values, liquidity=None, so_phien_tb=0):
    """Hàm thuần: index_points {symbol: {current, prev}} + exchange_values
    {group: value_vnd} + liquidity {group: pct} → payload chart. Index thiếu điểm
    vẫn ra row (điểm/%=None); thiếu nền lịch sử thì chỉ thanh_khoan_pct=None."""
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
                "thanh_khoan_pct": (liquidity or {}).get(cfg["group"]),
                "diem_tang_giam": change,
                "pct": pct,
            }
        )
    return {"indices": rows, "so_phien_tb": so_phien_tb}


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

    # Nền lịch sử dùng CHUNG snapshot với hai chart "5 phiên gần nhất" — không
    # phát sinh request nào, và sáng sớm nó đã được nạp từ đĩa (api.py:96).
    history = market_cache.get_snapshot("sector_flow_history") or {}
    avg_values, so_phien_tb = exchange_avg_values(history, exchange_map, vn30_members)
    today_values = exchange_today_values(
        board_rows, exchange_map, vn30_members, universe=set(history)
    )
    liquidity = liquidity_pct(today_values, avg_values)

    index_points = _index_points_cached()
    result = compute_index_overview(index_points, exchange_values, liquidity, so_phien_tb)
    result["generated_at"] = datetime.now(VN_TZ).isoformat()
    return result
