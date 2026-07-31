"""Chart "TOÀN CẢNH CHỈ SỐ" (cột nhóm) — 4 nhóm VN INDEX / HN INDEX / UP INDEX /
VN30, mỗi nhóm 3 cột: GIÁ TRỊ KHỚP LỆNH (nghìn tỷ), điểm tăng giảm, % tăng giảm.

Điểm chỉ số + đóng cửa phiên trước: lấy THẬT từ index history (close nến hôm nay
và nến áp chót).

Giá trị khớp lệnh mỗi sàn (2026-07-31 đổi): thanh khoản của TỪNG MÃ tính bằng
`giá × khối lượng` rồi cộng theo sàn (từ market_board_full + bản đồ sàn), VN30
cộng theo danh sách thành viên. Trước đó cột này cộng `value` (`accumulated_value`
của vendor).

Đánh đổi đã biết của phép đổi: `accumulated_value` là số THẬT, tính theo giá bình
quân của từng lệnh khớp trong phiên; còn `giá × khối lượng` coi như toàn bộ khối
lượng khớp ở giá HIỆN TẠI, nên là ƯỚC LƯỢNG — lệch lên khi giá cuối phiên cao hơn
giá bình quân và ngược lại. Đổi lại, cả hai cột của chart giờ dùng CHUNG một công
thức với nền lịch sử (`close × volume`), và mã nào vendor bỏ trống
`accumulated_value` vẫn có thanh khoản thay vì rơi về 0.

`thanh_khoan_pct` = giá trị GD hôm nay / TB các phiên trước × 100 (100% = đúng
bằng nền gần đây) — chart KHÔNG còn vẽ theo tỷ lệ này nữa, nó chỉ còn phục vụ
cột "% TB N phiên" của bảng bên dưới chart.

Hai cột dùng chung `exchange_today_values` nhưng KHÁC rổ mã: cột hiển thị cộng
TOÀN board (là con số tuyệt đối của cả sàn), còn tử số của tỷ lệ giới hạn theo
`universe` cho khớp mẫu số. Xem docstring hàm đó.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta

from app.data import data_source

logger = logging.getLogger(__name__)

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

# Điểm chỉ số: cache RAM + đĩa, do market_refresher._maybe_refresh_index_points
# làm mới ở luồng nền. KHÔNG có TTL nào ở đây: hết hạn giữa lúc vendor chậm là
# request phải gánh 4 call history × timeout cứng 30s của vnstock_data (tới ~2
# phút/request, FE báo lỗi → chart mất). Cache chỉ bị thay khi có bản MỚI.
_points_cache = {"at": 0.0, "data": {}}
_RUNTIME_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
)
INDEX_POINTS_CACHE_FILE = os.environ.get(
    "INDEX_POINTS_CACHE_FILE",
    os.path.join(_RUNTIME_DATA_DIR, "index_points_cache.json"),
)


def exchange_avg_values(history, exchange_map, vn30_members=None, today=None):
    """Giá trị GD trung bình mỗi nhóm qua các phiên ĐÃ ĐÓNG trong `history`.

    history: {symbol: [{time, close, volume}]} — snapshot sector_flow_history.
    Trả (avgs {group: vnd}, so_phien). ({}, 0) nếu không có phiên nào đã đóng.

    Nến history theo NGHÌN đồng (xem signal_service.PRICE_BOARD_SCALE) nên phải
    nhân _NGHIN_TO_VND — cùng cách flow_surge_service/sector_flow_service quy đổi.
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
    """Thanh khoản hôm nay mỗi nhóm = tổng `giá × khối lượng` (VND) của từng mã,
    cộng theo sàn (chuẩn hoá HSX→HOSE) + VN30 theo member list.

    Phục vụ CẢ HAI cột của chart, khác nhau ở `universe`:
      - `universe=None` → cộng TOÀN board: cột `gia_tri_khop_lenh` hiển thị, là
        con số tuyệt đối của cả sàn nên không được cắt bớt mã nào.
      - `universe=set(history)` → tử số của `thanh_khoan_pct`, giới hạn đúng rổ
        mã của mẫu số (board có cả chứng quyền/trái phiếu mà history thì không;
        tử rộng hơn mẫu sẽ làm tỷ lệ phồng lên).

    KHÔNG dùng `value` (accumulated_value) dù nó là số thật: nền lịch sử chỉ tính
    được bằng `close × volume`, trộn hai công thức làm tỷ lệ lệch hệ thống — giá
    đóng cửa khác giá bình quân phiên.
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


def _save_points_cache():
    try:
        with open(INDEX_POINTS_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "date": datetime.now(VN_TZ).date().isoformat(),
                    "points": _points_cache["data"],
                },
                f,
                ensure_ascii=False,
            )
    except OSError as e:
        logger.warning("ghi index_points cache that bai: %s", e)


def load_points_cache():
    """Nạp điểm chỉ số last-good từ đĩa lúc khởi động.

    Đây là snapshot DUY NHẤT của chart này chỉ sống trong RAM — board toàn TT,
    nến ngày, board VN100, thỏa thuận đều có cache đĩa (xem api.py lifespan).
    Không nạp lại thì restart qua đêm là cột "điểm tăng giảm"/"% tăng giảm"
    trắng cho tới khi vendor trả history, mà sáng sớm vendor hay chậm/bị chặn.
    """
    try:
        with open(INDEX_POINTS_CACHE_FILE, encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, ValueError) as e:
        logger.info("khong nap index_points cache tu dia: %s", e)
        return None
    raw = payload.get("points") if isinstance(payload, dict) else None
    if not isinstance(raw, dict):
        return None
    points = {}
    for symbol, pts in raw.items():
        if not isinstance(pts, dict) or not isinstance(pts.get("current"), (int, float)):
            continue
        prev = pts.get("prev")
        points[symbol] = {
            "current": float(pts["current"]),
            "prev": float(prev) if isinstance(prev, (int, float)) else None,
        }
    if not points:
        return None
    _points_cache["data"] = points
    _points_cache["at"] = time.monotonic()
    return points


def refresh_index_points(history_fn=None, now=None):
    """Làm mới điểm chỉ số vào cache RAM + đĩa. market_refresher gọi ở luồng nền.

    GỘP theo từng index thay vì gán cả dict: một lượt chỉ fetch được VNINDEX
    (VN30 lỗi/rate-limit) mà gán thẳng là VN30 mất khỏi chart dù vừa có số liệu
    tốt. Trả True khi có ít nhất một index mới.
    """
    fetched = _fetch_index_points(history_fn, now)
    if not fetched:
        return False
    _points_cache["data"] = {**_points_cache["data"], **fetched}
    _points_cache["at"] = time.monotonic()
    _save_points_cache()
    return True


def _index_points_cached(history_fn=None, now=None):
    """Điểm chỉ số cho request: chỉ ĐỌC cache. Cache trắng hẳn (boot lạnh, chưa
    có file đĩa, luồng nền chưa chạy lượt đầu) mới tự fetch một lần."""
    if _points_cache["data"]:
        return _points_cache["data"]
    refresh_index_points(history_fn, now)
    return _points_cache["data"]


def get_index_overview():
    """Điểm gọi runtime: value/sàn từ market_board_full (cache) + bản đồ sàn +
    VN30 members; điểm chỉ số fetch (memoize TTL). Không phụ thuộc luồng refresh."""
    from app.data import market_cache
    from app.services import vn100_service

    board_rows = market_cache.get_snapshot("market_board_full") or []
    exchange_map = data_source._symbol_exchange_map()
    vn30_members = vn100_service.get_vn30_members()
    # Cột hiển thị: Σ(giá × khối lượng) trên TOÀN board, không giới hạn rổ.
    exchange_values = exchange_today_values(board_rows, exchange_map, vn30_members)

    # Nền lịch sử dùng CHUNG snapshot với hai chart "5 phiên gần nhất" — không
    # phát sinh request nào, và sáng sớm nó đã được nạp từ đĩa (api.py:96).
    history = market_cache.get_snapshot("sector_flow_history") or {}
    avg_values, so_phien_tb = exchange_avg_values(history, exchange_map, vn30_members)
    # Tử số của tỷ lệ: CÙNG công thức nhưng cắt về đúng rổ mã của mẫu số.
    today_values = exchange_today_values(
        board_rows, exchange_map, vn30_members, universe=set(history)
    )
    liquidity = liquidity_pct(today_values, avg_values)

    index_points = _index_points_cached()
    result = compute_index_overview(index_points, exchange_values, liquidity, so_phien_tb)
    result["generated_at"] = datetime.now(VN_TZ).isoformat()
    return result
