"""Kho cache RAM dùng chung cho mọi user — endpoint chỉ ĐỌC, không gọi vnstock.

Hai loại cache:
- snapshot dùng chung (board_vn100, market_wide): luồng market_refresher ghi nền
  theo chu kỳ; getter trả bản tốt gần nhất (last-good) kể cả khi refresh lỗi →
  không bao giờ trả trắng nếu đã từng fetch được.
- nến intraday: cache theo (symbol, interval) có TTL + khóa single-flight để N
  request trùng mã/khung chỉ gọi vnstock 1 lần (chống đạp nhau khi đăng nhập đồng loạt).

Vì server chạy 1 process (xem thiết kế), cache để trong RAM module-level là chia
sẻ chung cho mọi request — không cần Redis.
"""

import threading
import time

from app.data import intraday_service


# ===== Snapshot dùng chung =====
# key -> {"data": <payload>, "fetched_at": float, "ok": bool}
_snapshots: dict = {}
_snap_lock = threading.Lock()


def set_snapshot(key, data, ok=True):
    """Ghi snapshot mới khi fetch thành công.

    ok=False (refresh lỗi/rate-limit) → GIỮ data cũ, chỉ đánh dấu stale; getter
    vẫn trả bản tốt gần nhất. Lần fetch đầu mà lỗi (chưa có data) → vẫn rỗng,
    endpoint tự fallback gọi service trực tiếp.
    """
    with _snap_lock:
        if not ok:
            cur = _snapshots.get(key)
            if cur is not None:
                cur["ok"] = False
            return
        _snapshots[key] = {"data": data, "fetched_at": time.time(), "ok": True}


def get_snapshot(key, default=None):
    """Trả data của snapshot (bản tốt gần nhất). Chưa có → default."""
    with _snap_lock:
        entry = _snapshots.get(key)
        return entry["data"] if entry else default


def snapshot_meta(key):
    """Metadata để chẩn đoán/health: {fetched_at, ok}. None nếu chưa có."""
    with _snap_lock:
        entry = _snapshots.get(key)
        if not entry:
            return None
        return {"fetched_at": entry["fetched_at"], "ok": entry["ok"]}


def reset():
    """Xóa toàn bộ cache — chỉ dùng trong test để cô lập trạng thái."""
    with _snap_lock:
        _snapshots.clear()
    with _intraday_lock:
        _intraday.clear()
        _intraday_locks.clear()


# ===== Nến intraday: TTL + single-flight =====
# FE poll /intraday mỗi ~6s (useIntraday.refetchInterval); TTL đặt ~5-6s để mỗi
# lần poll đều lấy nến mới thay vì phục vụ lại cache cũ. Nhờ single-flight, N user
# xem cùng mã vẫn chỉ 1 call vnstock/TTL.
INTRADAY_TTL_S = 5           # khung phút/giờ: đổi liên tục trong phiên
INTRADAY_DAILY_TTL_S = 6     # khung 1d/1w/1mth: nến đang hình thành vẫn đổi trong phiên
_DAILY_INTERVALS = {"1d", "1w", "1mth"}

# (symbol, interval) -> {"data": <dict endpoint>, "fetched_at": float}
_intraday: dict = {}
_intraday_lock = threading.Lock()   # bảo vệ _intraday + _intraday_locks
_intraday_locks: dict = {}          # (symbol, interval) -> Lock single-flight


def _ttl_for(interval):
    return INTRADAY_DAILY_TTL_S if interval in _DAILY_INTERVALS else INTRADAY_TTL_S


def _fresh(entry, ttl, now):
    return entry is not None and (now - entry["fetched_at"]) < ttl


def set_intraday(symbol, interval, result):
    """Ghi nến (dict {"data": [...]}) vào cache — dùng cho warm nền."""
    with _intraday_lock:
        _intraday[(symbol, interval)] = {"data": result, "fetched_at": time.time()}


def peek_intraday(symbol, interval):
    """Đọc bản intraday đang có trong RAM, không fetch mới dù cache hết TTL."""
    with _intraday_lock:
        entry = _intraday.get((symbol, interval))
        return entry["data"] if entry else None


def get_intraday(symbol, interval, fetch_fn=None):
    """Trả nến cho (symbol, interval) từ cache; miss/hết hạn → fetch đúng 1 lần.

    Single-flight theo key: nhiều request cùng mã/khung đang miss chỉ để 1 luồng
    đi fetch, số còn lại đợi khóa rồi đọc cache → không đạp vnstock. Fetch ra rỗng
    (lỗi/limit) mà đã có bản cũ → giữ bản cũ (last-good).

    Trả dict shape endpoint: {"data": [...]}.
    """
    key = (symbol, interval)
    ttl = _ttl_for(interval)

    with _intraday_lock:
        entry = _intraday.get(key)
        if _fresh(entry, ttl, time.time()):
            return entry["data"]
        lock = _intraday_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _intraday_locks[key] = lock

    with lock:
        # Double-check: luồng khác có thể vừa fetch xong trong lúc ta chờ khóa.
        with _intraday_lock:
            entry = _intraday.get(key)
            if _fresh(entry, ttl, time.time()):
                return entry["data"]

        fetch_fn = fetch_fn or intraday_service.get_intraday
        result = fetch_fn(symbol, interval)
        has_data = bool(result.get("data")) if isinstance(result, dict) else bool(result)
        if has_data:
            set_intraday(symbol, interval, result)
            return result

        # Fetch rỗng → trả bản cũ nếu có, không thì trả kết quả rỗng.
        with _intraday_lock:
            entry = _intraday.get(key)
        return entry["data"] if entry else result
