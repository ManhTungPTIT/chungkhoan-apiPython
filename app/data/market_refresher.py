"""Luồng nền refresh các snapshot dùng chung vào market_cache.

CHỈ luồng này gọi vnstock cho dữ liệu chung → số call không phụ thuộc số user.
Cháº¡y qua asyncio.to_thread (vnstock lÃ  call Ä‘á»“ng bá»™) Ä‘á»ƒ khÃ´ng cháº·n event loop,
theo Ä‘Ãºng pattern signal_service.scheduler_loop.

Ngân sách trong giờ GD: tick 1s × (4 price_board — chia theo sàn HSX/HNX/UPCOM,
UPCOM ~900 mÃ£ chia tiáº¿p 2 máº» â‰¤500 mÃ£ theo khuyáº¿n nghá»‹ vendor (xem
data_source.PRICE_BOARD_CHUNK_MAX) + 1 history náº¿n 1D VNINDEX cho /quotes)
= 300 call/phút — tick thường chỉ cập nhật snapshot quotes (/quotes); mỗi
tick thá»© 20 (~20s) tÃ¡i dÃ¹ng CÃ™NG láº§n fetch Ä‘Ã³ dá»±ng thÃªm views market_wide +
warm náº¿n VNINDEX (3 call/phÃºt, KHÃ”NG Ä‘á»•i so vá»›i báº£n 5s trÆ°á»›c â€” váº«n giá»¯ nhá»‹p
20s). Tá»•ng ~303 call/phÃºt â€” dÆ°á»›i háº¡n Golden 500 req/phÃºt, nhÆ°ng VÆ¯á»¢T háº¡n
Community 60 req/phÃºt (cháº¥p nháº­n Ä‘Æ°á»£c vÃ¬ project cháº¡y tier Golden, xem
vnstock_license; nếu rớt về Community phải tăng lại QUOTES_INTERVAL_S).
Danh sÃ¡ch mÃ£ (all_symbols) memoize theo ngÃ y, khÃ´ng tÃ­nh vÃ o ngÃ¢n sÃ¡ch trÃªn.
Board VN100 (price_board rá»• VNALL+HNX) KHÃ”NG cháº¡y má»—i chu ká»³ mÃ  giÃ£n ~1 tiáº¿ng/láº§n
(BOARD_VN100_INTERVAL_S) â€” sectors + heatmap Ä‘á»•i cháº­m, khÃ´ng cáº§n 20s. RiÃªng view
/vn100 (báº£ng giÃ¡ + signal) Ä‘Æ°á»£c dá»±ng láº¡i má»—i ~20s tá»« board toÃ n TT cá»§a
market_wide (_vn100_view) để signal cắt trong phiên không bị đóng băng theo giờ.
"""

import asyncio
import json
import logging
import math
import os
import threading
import time
from datetime import datetime, timedelta, timezone

from app.data import data_source
from app.services import foreign_trading_service
from app.services import homepage_service
from app.services import index_overview_service
from app.data import market_cache
from app.services import sector_service
from app.services import signal_service
from app.services import vn100_service

logger = logging.getLogger(__name__)

VN_TZ = timezone(timedelta(hours=7))

QUOTES_INTERVAL_S = 5      # tick nhanh trong giờ GD: cập nhật snapshot quotes
VIEWS_EVERY_TICKS = 20     # má»—i tick thá»© 20 (20Ã—1s = 20s) dá»±ng thÃªm views náº·ng
REALTIME_INTERVAL_S = QUOTES_INTERVAL_S * VIEWS_EVERY_TICKS  # nhá»‹p views (giá»¯ ~20s nhÆ° cÅ©)
IDLE_INTERVAL_S = 300      # ngoÃƒÂ i giÃ¡Â»Â: vÃ¡ÂºÂ«n refresh thÃ†Â°a Ã„â€˜Ã¡Â»Æ’ cÃƒÂ³ giÃƒÂ¡ Ã„â€˜ÃƒÂ³ng cÃ¡Â»Â­a mÃ¡Â»â€ºi nhÃ¡ÂºÂ¥t
MARKET_OPEN_HOUR = 9
MARKET_CLOSE_HOUR = 15     # tÃ¡Â»â€ºi 15:00 (giÃ¡Â»Â VN)

# Danh sÃƒÆ’Ã‚Â¡ch mÃƒÆ’Ã‚Â£ toÃƒÆ’Ã‚Â n TT memoize theo NGÃƒÆ’Ã¢â€šÂ¬Y: listing chÃƒÂ¡Ã‚Â»Ã¢â‚¬Â° Ãƒâ€žÃ¢â‚¬ËœÃƒÂ¡Ã‚Â»Ã¢â‚¬Â¢i khi cÃƒÆ’Ã‚Â³ niÃƒÆ’Ã‚Âªm yÃƒÂ¡Ã‚ÂºÃ‚Â¿t mÃƒÂ¡Ã‚Â»Ã¢â‚¬Âºi
# (rÃ¡ÂºÂ¥t hiÃ¡ÂºÂ¿m), trÃ†Â°Ã¡Â»â€ºc Ã„â€˜ÃƒÂ¢y fetch mÃ¡Â»â€”i chu kÃ¡Â»Â³ 20s = 3 call/phÃƒÂºt vÃƒÂ´ ÃƒÂ­ch.
_all_symbols: list[str] = []
_all_symbols_date = None   # ngÃƒÂ y (giÃ¡Â»Â VN) Ã„â€˜ÃƒÂ£ fetch danh sÃƒÂ¡ch thÃƒÂ nh cÃƒÂ´ng

# Board VN100 (price_board rÃƒÂ¡Ã‚Â»Ã¢â‚¬Â¢ VNALL+HNX ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ bÃƒÂ¡Ã‚ÂºÃ‚Â£ng giÃƒÆ’Ã‚Â¡/sectors/heatmap) refresh giÃƒÆ’Ã‚Â£n
# ~1 tiÃ¡ÂºÂ¿ng/lÃ¡ÂºÂ§n thay vÃƒÂ¬ mÃ¡Â»â€”i chu kÃ¡Â»Â³ 20s: dÃ¡Â»Â¯ liÃ¡Â»â€¡u Ã„â€˜Ã¡Â»â€¢i chÃ¡ÂºÂ­m, tiÃ¡ÂºÂ¿t kiÃ¡Â»â€¡m ~3 call/phÃƒÂºt.
BOARD_VN100_INTERVAL_S = 3600
_last_board_vn100_at = 0.0  # time.monotonic() lÃƒÂ¡Ã‚ÂºÃ‚Â§n refresh board VN100 gÃƒÂ¡Ã‚ÂºÃ‚Â§n nhÃƒÂ¡Ã‚ÂºÃ‚Â¥t (thÃƒÆ’Ã‚Â nh cÃƒÆ’Ã‚Â´ng)
# Bảng thỏa thuận: 3 request/lượt (HOSE+HNX+UPCOM). Lệnh thỏa thuận thưa (vài
# chá»¥c lá»‡nh/phiÃªn) nÃªn 60s lÃ  quÃ¡ Ä‘á»§, khÃ´ng cáº§n bÃ¡m nhá»‹p views 20s.
PUT_THROUGH_INTERVAL_S = 60
_last_put_through_at = 0.0
RUNTIME_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
)
BOARD_VN100_CACHE_FILE = os.environ.get(
    "BOARD_VN100_CACHE_FILE",
    os.path.join(RUNTIME_DATA_DIR, "board_vn100_cache.json"),
)
# NÃƒÂ¡Ã‚ÂºÃ‚Â¿n dÃƒÂ¡Ã‚Â»Ã‚Â±ng sÃƒÂ¡Ã‚ÂºÃ‚Âµn cho trang mÃƒÂ¡Ã‚ÂºÃ‚Â·c Ãƒâ€žÃ¢â‚¬ËœÃƒÂ¡Ã‚Â»Ã¢â‚¬Â¹nh (FE mÃƒÂ¡Ã‚Â»Ã…Â¸ VNINDEX khung 1d khi khÃƒÂ¡Ã‚Â»Ã…Â¸i Ãƒâ€žÃ¢â‚¬ËœÃƒÂ¡Ã‚Â»Ã¢â€žÂ¢ng).
WARM_INTRADAY = [("VNINDEX", "1d")]

_refresh_lock = threading.Lock()
_prev_volume_date = None   # ngÃƒÆ’Ã‚Â y Ãƒâ€žÃ¢â‚¬ËœÃƒÆ’Ã‚Â£ tÃƒÆ’Ã‚Â­nh prev_total_volume (chÃƒÂ¡Ã‚Â»Ã¢â‚¬Â° Ãƒâ€žÃ¢â‚¬ËœÃƒÂ¡Ã‚Â»Ã¢â‚¬Â¢i theo ngÃƒÆ’Ã‚Â y)
_prev_total_volume = 0


def is_market_hours(now=None):
    """T2Ã¢â‚¬â€œT6, 09:00 Ã¢â€°Â¤ giÃ¡Â»Â < 15:00 (giÃ¡Â»Â VN). NgoÃƒÂ i khoÃ¡ÂºÂ£ng nÃƒÂ y coi nhÃ†Â° ngoÃƒÂ i phiÃƒÂªn."""
    now = now or datetime.now(VN_TZ)
    if now.weekday() >= 5:  # 5=T7, 6=CN
        return False
    return MARKET_OPEN_HOUR <= now.hour < MARKET_CLOSE_HOUR


def _prev_total_volume_today():
    """KL khÃ¡Â»â€ºp phiÃƒÂªn hÃƒÂ´m qua Ã¢â‚¬â€ chÃ¡Â»â€° Ã„â€˜Ã¡Â»â€¢i theo ngÃƒÂ y nÃƒÂªn tÃƒÂ­nh 1 lÃ¡ÂºÂ§n/ngÃƒÂ y (3 history call)."""
    global _prev_volume_date, _prev_total_volume
    today = datetime.now(VN_TZ).strftime("%Y-%m-%d")
    if _prev_volume_date != today:
        _prev_total_volume = homepage_service.prev_total_volume()
        _prev_volume_date = today
    return _prev_total_volume


def _board_has_trades(board) -> bool:
    """Board TRƯỚC GIỜ MỞ PHIÊN: feed reset accumulated_value theo ngày nên mọi
    mã đều value=0 dù board đầy row. Các view lọc/scale theo value (power, vn100,
    heatmap) dá»±ng tá»« board nÃ y sáº½ rá»—ng/0 â†’ pháº£i coi lÃ  stale Ä‘á»ƒ giá»¯ dá»¯ liá»‡u
    PHIÃŠN TRÆ¯á»šC (last-good) thay vÃ¬ ghi Ä‘Ã¨ báº±ng báº£n tráº¯ng (bug tráº¯ng trang
    power/heatmap/bộ lọc sáng hôm sau)."""
    return any((r.get("value") or 0) > 0 for r in board)


def _build_board_vn100_snapshot(board):
    groups = sector_service.build_groups(board)
    return {
        "vn100": vn100_service.build_board(board),
        "sectors": sector_service.sectors_view(groups),
        "heatmap": sector_service.heatmap_view(groups),
        "groups": groups,
    }


def refresh_board_vn100():
    """1 request price_board(VN100) -> build cached vn100/sectors/heatmap views.

    Empty/error/pre-open fetches are treated as stale so last-good data is
    preserved (xem _board_has_trades).
    """
    symbols = vn100_service.get_symbols()
    if not symbols:
        market_cache.set_snapshot("board_vn100", None, ok=False)
        return False
    board = data_source.fetch_vn100_board(symbols)
    if not board:
        market_cache.set_snapshot("board_vn100", None, ok=False)
        return False
    if not _board_has_trades(board):
        if market_cache.get_snapshot("board_vn100"):
            # Trước giờ mở phiên: giữ snapshot phiên trước (RAM lẫn cache đĩa).
            market_cache.set_snapshot("board_vn100", None, ok=False)
            return False
        # ChÆ°a tá»«ng cÃ³ dá»¯ liá»‡u (boot láº§n Ä‘áº§u trÆ°á»›c phiÃªn): váº«n dá»±ng khung snapshot
        # Ä‘á»ƒ /sectors, /heatmap cÃ³ nhÃ³m ngÃ nh thay vÃ¬ trá»‘ng háº³n, nhÆ°ng KHÃ”NG ghi
        # đĩa và trả False để không dời mốc ~1 tiếng — chu kỳ sau thử lại, vào
        # phiÃªn lÃ  cÃ³ dá»¯ liá»‡u tháº­t ngay.
        market_cache.set_snapshot("board_vn100", _build_board_vn100_snapshot(board))
        return False
    snapshot = _build_board_vn100_snapshot(board)
    # Đính kèm breadth/depth (đã dựng sẵn trong market_wide mỗi ~20s — 0 call
    # thêm) vào cache đĩa: boot trước phiên đọc lại để /homepage/market-breadth
    # vÃ  market-depth hiá»‡n sá»‘ phiÃªn trÆ°á»›c thay vÃ¬ 0 (cÃ¹ng tinh tháº§n power).
    # ChÆ°a cÃ³ market_wide (boot Ä‘áº§u phiÃªn, bÆ°á»›c nÃ y cháº¡y trÆ°á»›c refresh_tick
    # trong refresh_all) → bỏ qua, lần save hàng giờ kế tiếp sẽ có.
    wide = market_cache.get_snapshot("market_wide") or {}
    if wide.get("breadth"):
        snapshot["breadth"] = wide["breadth"]
    if wide.get("depth"):
        snapshot["depth"] = wide["depth"]
    market_cache.set_snapshot("board_vn100", snapshot)
    _save_board_vn100_cache(snapshot)
    return True


def _save_board_vn100_cache(snapshot):
    try:
        with open(BOARD_VN100_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False)
    except OSError as e:
        logger.warning("ghi board_vn100 cache that bai: %s", e)


def load_board_vn100_cache():
    """Load last-good board_vn100 from disk before the first vnstock refresh."""
    try:
        with open(BOARD_VN100_CACHE_FILE, encoding="utf-8") as f:
            snapshot = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.info("khong nap board_vn100 cache tu dia: %s", e)
        return None
    if not isinstance(snapshot, dict) or "heatmap" not in snapshot:
        logger.warning("board_vn100 cache tren dia khong hop le")
        return None
    market_cache.set_snapshot("board_vn100", snapshot)
    return snapshot


# Board TOÀN thị trường (cho flow_surge). Persist ra đĩa + nạp lúc boot để CHART
# vẫn hiển thị (đủ universe rộng) khi hết phiên / sáng hôm sau trước giờ mở, kể
# cáº£ sau restart qua Ä‘Ãªm â€” RAM last-good máº¥t khi restart. Ghi throttled (khÃ´ng
# má»—i tick ~20s) vÃ¬ payload full board lá»›n hÆ¡n board_vn100.
MARKET_BOARD_FULL_CACHE_FILE = os.environ.get(
    "MARKET_BOARD_FULL_CACHE_FILE",
    os.path.join(RUNTIME_DATA_DIR, "market_board_full_cache.json"),
)
FULL_BOARD_SAVE_INTERVAL_S = 600  # ghi Ä‘Ä©a tá»‘i Ä‘a má»—i ~10 phÃºt
_last_full_board_save_at = 0.0


def _save_market_board_full_cache(rows):
    try:
        with open(MARKET_BOARD_FULL_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False)
    except OSError as e:
        logger.warning("ghi market_board_full cache that bai: %s", e)


def load_market_board_full_cache():
    """Náº¡p last-good board toÃ n TT tá»« Ä‘Ä©a trÆ°á»›c láº§n fetch vnstock Ä‘áº§u â€” Ä‘á»ƒ
    flow_surge có universe rộng ngay khi boot ngoài giờ giao dịch."""
    try:
        with open(MARKET_BOARD_FULL_CACHE_FILE, encoding="utf-8") as f:
            rows = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.info("khong nap market_board_full cache tu dia: %s", e)
        return None
    if not isinstance(rows, list) or not rows:
        return None
    market_cache.set_snapshot("market_board_full", rows)
    return rows

def _maybe_refresh_board_vn100(now=None):
    """Refresh board VN100 tÃ¡Â»â€˜i Ã„â€˜a mÃ¡Â»â€”i BOARD_VN100_INTERVAL_S (~1 tiÃ¡ÂºÂ¿ng). ChÃ¡Â»â€° dÃ¡Â»Âi
    mÃ¡Â»â€˜c khi refresh THÃƒâ‚¬NH CÃƒâ€NG Ã¢â€ â€™ lÃ¡ÂºÂ§n lÃ¡Â»â€”i (rate-limit) Ã„â€˜Ã†Â°Ã¡Â»Â£c thÃ¡Â»Â­ lÃ¡ÂºÂ¡i Ã¡Â»Å¸ chu kÃ¡Â»Â³ 20s kÃ¡ÂºÂ¿
    thay vÃƒÂ¬ Ã„â€˜Ã¡Â»Â£i trÃ¡Â»Ân 1 tiÃ¡ÂºÂ¿ng vÃ¡Â»â€ºi bÃ¡ÂºÂ£ng giÃƒÂ¡ cÃ…Â©."""
    global _last_board_vn100_at
    now = now if now is not None else time.monotonic()
    if now - _last_board_vn100_at < BOARD_VN100_INTERVAL_S:
        return
    if refresh_board_vn100():
        _last_board_vn100_at = now


SECTOR_FLOW_CACHE_FILE = os.environ.get(
    "SECTOR_FLOW_CACHE_FILE",
    os.path.join(RUNTIME_DATA_DIR, "sector_flow_history_cache.json"),
)
# Số luồng fetch history song song. Đo 23/07/2026: 8 luồng ~4 phút cho 1.596 mã
# (16 luá»“ng chá»‰ nhanh hÆ¡n ~10%, khÃ´ng Ä‘Ã¡ng Ä‘Ã¡nh Ä‘á»•i rá»§i ro bá»‹ cháº·n).
SECTOR_FLOW_WORKERS = 8
# Chá»‰ cáº§n ~5 phiÃªn gáº§n nháº¥t; láº¥y dÆ° 20 ngÃ y lá»‹ch Ä‘á»ƒ cháº¯c cháº¯n vÆ°á»£t cuá»‘i tuáº§n vÃ 
# nghá»‰ lá»… dÃ i.
SECTOR_FLOW_LOOKBACK_DAYS = 20
_sector_flow_date = None  # ngày (giờ VN) đã nạp history toàn thị trường xong


def _finite(value):
    """Chuá»—i vendor â†’ float, None náº¿u khÃ´ng pháº£i sá»‘ há»¯u háº¡n ('nan', '', None)."""
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _normalize_candles(rows):
    """Nến ngày rút gọn cho snapshot `sector_flow_history`, close/volume ép về SỐ.

    `data_source._map_history` ép cả DataFrame về chuỗi (`df.astype(str)`) nên
    vendor tráº£ close "25.5" / volume "1101". Consumer nhÃ¢n tháº³ng close Ã— volume
    (index_overview_service) sáº½ ná»• TypeError. Ã‰p ngay táº¡i chá»— sinh snapshot â€”
    Ä‘Ãºng cÃ¡ch signal_service lÃ m khi dá»±ng `_history_candles` â€” Ä‘á»ƒ khÃ´ng service
    nÃ o pháº£i tá»± nhá»› Ã©p kiá»ƒu.

    Bỏ nến thiếu time hoặc close hỏng ('nan' khi vendor pad ngày nghỉ); volume
    thiếu tính là 0 (không có giao dịch), giống cách các service đọc nến.
    """
    out = []
    for row in rows or []:
        time_value = row.get("time")
        close = _finite(row.get("close"))
        if not time_value or close is None:
            continue
        out.append(
            {
                "time": time_value,
                "close": close,
                "volume": int(_finite(row.get("volume")) or 0),
            }
        )
    return out


def _save_sector_flow_cache(history):
    try:
        with open(SECTOR_FLOW_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False)
    except OSError as e:
        logger.warning("ghi sector flow cache tháº¥t báº¡i: %s", e)


def load_sector_flow_cache():
    """Náº¡p láº¡i history 5 phiÃªn tá»« Ä‘Ä©a lÃºc khá»Ÿi Ä‘á»™ng â€” náº¿u khÃ´ng, má»—i láº§n restart
    láº¡i pháº£i quÃ©t ~1.600 mÃ£ (~4 phÃºt) trÆ°á»›c khi hai chart 5 phiÃªn cÃ³ dá»¯ liá»‡u."""
    global _sector_flow_date
    try:
        with open(SECTOR_FLOW_CACHE_FILE, encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, ValueError):
        return None
    raw = payload.get("history") if isinstance(payload, dict) else None
    if not raw:
        return None
    # Cache ghi trÆ°á»›c báº£n vÃ¡ kiá»ƒu dá»¯ liá»‡u váº«n cÃ²n chuá»—i, mÃ  restart thÃ¬ náº¡p tháº³ng
    # từ đĩa chứ không chạy lại refresh — không chuẩn hoá ở đây là chart hỏng tới
    # táº­n lÆ°á»£t quÃ©t ngÃ y hÃ´m sau.
    history = {}
    for symbol, rows in raw.items():
        candles = _normalize_candles(rows)
        if candles:
            history[symbol] = candles
    if not history:
        return None
    market_cache.set_snapshot("sector_flow_history", history)
    _sector_flow_date = payload.get("date")
    return history


_DAY_S = 86400


def _session_epoch(day) -> int:
    """Mốc thời gian phiên `day` theo convention nến vendor: 00:00 UTC của NGÀY
    GIAO DỊCH (VCI trả timestamp tz-aware UTC).

    Trước đây hàm này lấy 00:00 GIỜ VN = 17:00 UTC hôm trước, lệch đúng 7 tiếng
    so với nến history. Hệ quả: cột phiên hiện tại nằm ở mốc riêng nên
    `sessionLabel` của FE (đọc theo UTC) in ra ngày HÔM QUA — chart hiện 5 cột
    nhưng hai cột cuối trùng nhãn, trông như thiếu phiên hiện tại.
    """
    return int(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp())


def _session_date(candle_time):
    """Ngày giao dịch của một mốc nến. Đọc theo giờ VN nên đúng với CẢ HAI
    convention (00:00 UTC → 07:00 VN cùng ngày; 00:00 VN → cùng ngày)."""
    try:
        return datetime.fromtimestamp(int(candle_time), VN_TZ).date()
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _aligned_session_epoch(history, day) -> int:
    """Mốc cho phiên `day` CĂN theo chính các nến đang có trong history.

    Lấy phiên đã đóng gần nhất rồi cộng số ngày chênh lệch: khoảng cách giữa các
    mốc nến luôn là bội số nguyên của 86400 dù vendor dùng convention nào, nên
    cách này tự khớp và không phải đoán múi giờ của vendor. History rỗng (hoặc
    chỉ có nến hôm nay từ lần merge trước) → rơi về `_session_epoch`.
    """
    last = 0
    for rows in (history or {}).values():
        for candle in rows or []:
            time_value = int(candle.get("time") or 0)
            if time_value > last and (_session_date(time_value) or day) < day:
                last = time_value
    if last <= 0:
        return _session_epoch(day)
    last_date = _session_date(last)
    if last_date is None:
        return _session_epoch(day)
    return last + _DAY_S * (day - last_date).days


def _today_board_candles(board, today=None, session_time=None) -> dict:
    """Nến phiên hiện tại dựng từ price_board realtime cho sector-flow.

    History daily của vendor thường cập nhật muộn hơn board trong phiên. Board đã có
    `value` khớp lệnh tích lũy theo VND, nên dùng trực tiếp để 2 chart 5 phiên có
    cột hôm nay thay vì đứng ở phiên hôm qua.
    """
    if not board or not _board_has_trades(board):
        return {}
    today = today or datetime.now(VN_TZ).date()
    session_time = _session_epoch(today) if session_time is None else session_time
    candles = {}
    for row in board or []:
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            continue
        price = _finite(row.get("price")) or 0.0
        value = _finite(row.get("value")) or 0.0
        if price <= 0 or value <= 0:
            continue
        candles[symbol] = {
            "time": session_time,
            "close": price / 1000,
            "volume": int(_finite(row.get("volume")) or 0),
            "value": value,
        }
    return candles


def _merge_board_session_into_sector_flow_history(board=None, today=None) -> bool:
    history = market_cache.get_snapshot("sector_flow_history") or {}
    if not history:
        return False
    today = today or datetime.now(VN_TZ).date()
    session_time = _aligned_session_epoch(history, today)
    candles = _today_board_candles(
        board if board is not None else market_cache.get_snapshot("market_board_full"),
        today=today,
        session_time=session_time,
    )
    if not candles:
        return False

    merged = dict(history)
    for symbol, candle in candles.items():
        rows = list(merged.get(symbol) or [])
        # Bỏ theo NGÀY chứ không theo mốc: cache cũ (hoặc nến vendor về muộn
        # trong ngày) có thể mang mốc khác cho cùng phiên hôm nay — lọc theo mốc
        # sẽ để lại hai cột cùng một ngày.
        rows = [r for r in rows if _session_date(r.get("time")) != today]
        rows.append(candle)
        rows.sort(key=lambda r: int(r.get("time") or 0))
        merged[symbol] = rows

    market_cache.set_snapshot("sector_flow_history", merged)
    return True


def _merge_current_board_session_into_sector_flow_history(today=None) -> bool:
    meta = market_cache.snapshot_meta("market_board_full") or {}
    if meta.get("ok") is False:
        return False
    return _merge_board_session_into_sector_flow_history(today=today)

def refresh_sector_flow():
    """Nến ngày TOÀN thị trường cho hai chart "5 phiên gần nhất".

    Đây là luồng nặng nhất của hệ thống (~1.600 request history) nên chỉ chạy MỘT
    LẦN MỖI NGÀY và fetch song song. Mã lỗi lẻ tẻ được bỏ qua chứ không retry:
    thiáº¿u vÃ i mÃ£ chá»‰ lÃ m lá»‡ch nháº¹ tá»•ng ngÃ nh, cÃ²n quÃ©t láº¡i cáº£ rá»• thÃ¬ tá»‘n thÃªm vÃ i
    phÃºt. Ghi ra Ä‘Ä©a Ä‘á»ƒ restart khÃ´ng pháº£i quÃ©t láº¡i.
    """
    from concurrent.futures import ThreadPoolExecutor

    symbols = _all_symbols_today()
    if not symbols:
        return False
    # Chỉ mã cổ phiếu 3 ký tự: rổ niêm yết còn lẫn trái phiếu/chứng quyền, mà
    # history của chúng vô nghĩa với dòng tiền theo ngành.
    symbols = [s for s in symbols if len(s) == 3 and s.isalpha()]

    today = datetime.now(VN_TZ).date()
    start = (today - timedelta(days=SECTOR_FLOW_LOOKBACK_DAYS)).isoformat()
    end = today.isoformat()

    def fetch(symbol):
        rows = data_source.fetch_intraday_history(symbol, start, end, "1D")
        return symbol, rows or []

    history = {}
    with ThreadPoolExecutor(max_workers=SECTOR_FLOW_WORKERS) as pool:
        for symbol, rows in pool.map(fetch, symbols):
            candles = _normalize_candles(rows)
            if candles:
                history[symbol] = candles

    if not history:
        return False
    market_cache.set_snapshot("sector_flow_history", history)
    _merge_current_board_session_into_sector_flow_history(today=today)
    history = market_cache.get_snapshot("sector_flow_history") or history
    _save_sector_flow_cache({"date": today.isoformat(), "history": history})
    logger.info("sector flow history: %d/%d mÃ£", len(history), len(symbols))
    return True


def _maybe_refresh_sector_flow(now=None):
    """Chạy tối đa 1 lần/ngày. Chỉ dời mốc khi THÀNH CÔNG → hôm nào lỗi thì lượt
    refresh sau thá»­ láº¡i thay vÃ¬ Ä‘á»£i sang ngÃ y má»›i."""
    global _sector_flow_date
    today = (now or datetime.now(VN_TZ)).date().isoformat()
    if _sector_flow_date == today:
        return
    if refresh_sector_flow():
        _sector_flow_date = today


_foreign_backfill_thread = None


def backfill_foreign_history():
    """Quet ~1.575 ma de dung lai lich su khoi ngoai TOAN thi truong."""
    symbols = _all_symbols_today()
    if not symbols:
        return
    # Cung bo loc voi refresh_sector_flow: ro niem yet con lan trai phieu/chung
    # quyen, khoi ngoai cua chung khong thuoc thong ke nay.
    symbols = [s for s in symbols if len(s) == 3 and s.isalpha()]
    rows = foreign_trading_service.fetch_history_from_vendor(symbols)
    if foreign_trading_service.apply_backfill(rows):
        logger.info("backfill khoi ngoai: %d phien tu %d ma", len(rows), len(symbols))
    else:
        logger.warning("backfill khoi ngoai that bai — luot refresh sau thu lai")


def _maybe_backfill_foreign_history(now=None):
    """Backfill lich su khoi ngoai (chart "GIAO DICH KHOI NGOAI 30 PHIEN").

    Chi chay khi lich su dang thung — ngay thuong update_history_from_board da
    boi dung mot dong moi phien tu board co san, khong ton request nao.

    Chay o LUONG RIENG, khong om _refresh_lock: mot luot quet mat ~8 phut (nhip
    bi bop ve 200 request/phut cho khoi duoi tran 500/phut cua vendor), om khoa
    thi quotes va board dung hinh nguyen ca 8 phut do. Nhip 200/phut da chua
    thua han muc cho cac luong refresh chay song song."""
    global _foreign_backfill_thread
    if _foreign_backfill_thread is not None and _foreign_backfill_thread.is_alive():
        return
    if not foreign_trading_service.needs_backfill(now=now):
        return
    _foreign_backfill_thread = threading.Thread(
        target=_run_step,
        args=(backfill_foreign_history,),
        name="foreign-backfill",
        daemon=True,
    )
    _foreign_backfill_thread.start()


# Điểm chỉ số (chart "TOÀN CẢNH CHỈ SỐ"): 4 call history/lượt, điểm đổi chậm nên
# 60s là đủ — giữ đúng ngân sách của bản cũ (memoize TTL 60s) nhưng dời việc fetch
# từ ĐƯỜNG REQUEST sang luồng nền này.
INDEX_POINTS_INTERVAL_S = 60
_last_index_points_at = 0.0


def _maybe_refresh_index_points(now=None):
    """Náº¡p Ä‘iá»ƒm chá»‰ sá»‘ vÃ o cache cá»§a index_overview_service má»—i
    INDEX_POINTS_INTERVAL_S. Chỉ dời mốc khi THÀNH CÔNG → lượt refresh sau thử
    lại ngay thay vì đợi trọn 60s với dữ liệu cũ (giống
    _maybe_refresh_board_vn100)."""
    global _last_index_points_at
    now_mono = time.monotonic()
    if _last_index_points_at and now_mono - _last_index_points_at < INDEX_POINTS_INTERVAL_S:
        return
    if index_overview_service.refresh_index_points(now=now):
        _last_index_points_at = now_mono


PUT_THROUGH_CACHE_FILE = os.environ.get(
    "PUT_THROUGH_CACHE_FILE",
    os.path.join(RUNTIME_DATA_DIR, "put_through_cache.json"),
)


def _save_put_through_cache(deals):
    try:
        with open(PUT_THROUGH_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(deals, f, ensure_ascii=False)
    except OSError as e:
        logger.warning("ghi put_through cache tháº¥t báº¡i: %s", e)


def load_put_through_cache():
    """Nạp lệnh thỏa thuận phiên gần nhất từ đĩa lúc khởi động — RAM last-good
    mất sau restart, mà bảng thỏa thuận sáng sớm chưa có lệnh nào."""
    try:
        with open(PUT_THROUGH_CACHE_FILE, encoding="utf-8") as f:
            deals = json.load(f)
    except (OSError, ValueError):
        return None
    if not deals:
        return None
    market_cache.set_snapshot("put_through", deals)
    return deals


def refresh_put_through(fetch_fn=None):
    """Snapshot `put_through`: TỪNG lệnh thỏa thuận cả 3 sàn (chưa gom, chưa lọc
    ngÆ°á»¡ng) Ä‘á»ƒ endpoint Ä‘á»•i min_value khÃ´ng pháº£i fetch láº¡i.

    Hai trường hợp đều GIỮ bản tốt gần nhất, vì cùng một lý do — giá trị thỏa
    thuận chỉ tăng dần trong phiên nên bản cũ vẫn đọc được, còn ghi đè bằng rỗng
    thÃ¬ biá»ƒu Ä‘á»“ tráº¯ng:
      - fetch hỏng cả 3 sàn (None) → đánh dấu stale;
      - vendor trả RỖNG hợp lệ (sáng sớm chưa ai giao dịch thỏa thuận) → im lặng
        giá»¯ nguyÃªn. Lá»‡nh Ä‘áº§u tiÃªn cá»§a phiÃªn má»›i sáº½ tá»± ghi Ä‘Ã¨.
    ChÆ°a tá»«ng cÃ³ dá»¯ liá»‡u thÃ¬ [] má»›i Ä‘Æ°á»£c ghi vÃ o, Ä‘á»ƒ chart hiá»‡n tráº¡ng thÃ¡i rá»—ng
    thay vÃ¬ treo mÃ£i á»Ÿ "Ä‘ang táº£i".
    """
    deals = (fetch_fn or data_source.fetch_put_through)()
    if deals is None:
        market_cache.set_snapshot("put_through", None, ok=False)
        return False
    if not deals and market_cache.get_snapshot("put_through"):
        return True
    market_cache.set_snapshot("put_through", deals)
    if deals:
        _save_put_through_cache(deals)
    return True


def _maybe_refresh_put_through(now=None):
    """Giãn nhịp bảng thỏa thuận về PUT_THROUGH_INTERVAL_S. Chỉ dời mốc khi
    refresh THÃ€NH CÃ”NG â†’ láº§n lá»—i Ä‘Æ°á»£c thá»­ láº¡i á»Ÿ lÆ°á»£t káº¿ (self-heal)."""
    global _last_put_through_at
    now = now if now is not None else time.monotonic()
    if now - _last_put_through_at < PUT_THROUGH_INTERVAL_S:
        return
    if refresh_put_through():
        _last_put_through_at = now


def _all_symbols_today():
    """Danh sÃƒÂ¡ch mÃƒÂ£ toÃƒÂ n TT, memoize theo ngÃƒÂ y. Fetch lÃ¡Â»â€”i Ã¢â€ â€™ trÃ¡ÂºÂ£ bÃ¡ÂºÂ£n cÃ…Â© nÃ¡ÂºÂ¿u cÃƒÂ³
    (stale vÃ¡ÂºÂ«n hÃ†Â¡n rÃ¡Â»â€”ng Ã¢â‚¬â€ rÃ¡Â»â€¢ niÃƒÂªm yÃ¡ÂºÂ¿t gÃ¡ÂºÂ§n nhÃ†Â° khÃƒÂ´ng Ã„â€˜Ã¡Â»â€¢i trong ngÃƒÂ y), memo date
    khÃƒÂ´ng dÃ¡Â»Âi nÃƒÂªn tick sau tÃ¡Â»Â± thÃ¡Â»Â­ lÃ¡ÂºÂ¡i (self-heal)."""
    global _all_symbols, _all_symbols_date
    today = datetime.now(VN_TZ).strftime("%Y-%m-%d")
    if _all_symbols and _all_symbols_date == today:
        return _all_symbols
    fetched = data_source.fetch_all_symbols()
    if fetched:
        _all_symbols = fetched
        _all_symbols_date = today
    return _all_symbols


# Index cÃƒÂ¡Ã‚ÂºÃ‚Â§n giÃƒÆ’Ã‚Â¡ realtime trong /quotes (mÃƒÆ’Ã‚Â n hÃƒÆ’Ã‚Â¬nh mÃƒÂ¡Ã‚ÂºÃ‚Â·c Ãƒâ€žÃ¢â‚¬ËœÃƒÂ¡Ã‚Â»Ã¢â‚¬Â¹nh FE mÃƒÂ¡Ã‚Â»Ã…Â¸ VNINDEX). Index
# khÃƒÆ’Ã‚Â´ng cÃƒÆ’Ã‚Â³ trong price_board (board chÃƒÂ¡Ã‚Â»Ã¢â‚¬Â° cÃƒÂ¡Ã‚Â»Ã¢â‚¬Â¢ phiÃƒÂ¡Ã‚ÂºÃ‚Â¿u) ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ fetch riÃƒÆ’Ã‚Âªng qua history.
INDEX_QUOTE_SYMBOLS = ["VNINDEX"]


def _fetch_index_quotes():
    """GiÃƒÆ’Ã‚Â¡ index cho /quotes: nÃƒÂ¡Ã‚ÂºÃ‚Â¿n 1D cÃƒÂ¡Ã‚Â»Ã‚Â§a RIÃƒÆ’Ã…Â NG hÃƒÆ’Ã‚Â´m nay (1 call/index, trÃƒÂ¡Ã‚ÂºÃ‚Â£ 1 nÃƒÂ¡Ã‚ÂºÃ‚Â¿n)
    Ã¢â‚¬â€ close = Ã„â€˜iÃ¡Â»Æ’m hiÃ¡Â»â€¡n tÃ¡ÂºÂ¡i, volume = KL khÃ¡Â»â€ºp cÃ¡Â»â„¢ng dÃ¡Â»â€œn phiÃƒÂªn. NguÃ¡Â»â€œn history nÃƒÂªn
    Ã„â€˜Ã†Â¡n vÃ¡Â»â€¹ lÃƒÂ  Ã„ÂIÃ¡Â»â€šM, khÃ¡Â»â€ºp sÃ¡ÂºÂµn nÃ¡ÂºÂ¿n /intraday cÃ¡Â»Â§a index (KHÃƒâ€NG chia
    PRICE_BOARD_SCALE nhÃ†Â° cÃ¡Â»â€¢ phiÃ¡ÂºÂ¿u). Fetch lÃ¡Â»â€”i / nÃ¡ÂºÂ¿n rÃ¡Â»â€”ng (cuÃ¡Â»â€˜i tuÃ¡ÂºÂ§n, trÃ†Â°Ã¡Â»â€ºc
    phiÃƒÂªn) Ã¢â€ â€™ bÃ¡Â»Â mÃƒÂ£ Ã„â€˜ÃƒÂ³ Ã¢â‚¬â€ index thiÃ¡ÂºÂ¿u khÃƒÂ´ng chÃ¡ÂºÂ·n quotes cÃ¡Â»â€¢ phiÃ¡ÂºÂ¿u."""
    today = datetime.now(VN_TZ).strftime("%Y-%m-%d")
    quotes = {}
    for symbol in INDEX_QUOTE_SYMBOLS:
        rows = data_source.fetch_intraday_history(symbol, today, today, "1D")
        if not rows:
            continue
        last = rows[-1]
        try:
            price = float(last["close"])
        except (KeyError, TypeError, ValueError):
            continue
        if price <= 0:
            continue
        quotes[symbol] = {"price": price, "volume": last.get("volume", 0)}
    return quotes


def _build_quotes(board, index_quotes=None):
    """Payload /quotes: {"time": ISO giÃ¡Â»Â VN, "data": {symbol: {price, volume}}}.

    GiÃƒÆ’Ã‚Â¡ cÃƒÂ¡Ã‚Â»Ã¢â‚¬Â¢ phiÃƒÂ¡Ã‚ÂºÃ‚Â¿u tÃƒÂ¡Ã‚Â»Ã‚Â« price_board lÃƒÆ’Ã‚Â  VND thÃƒÆ’Ã‚Â´ (vd 62900) cÃƒÆ’Ã‚Â²n nÃƒÂ¡Ã‚ÂºÃ‚Â¿n /intraday theo
    nghÃƒÆ’Ã‚Â¬n Ãƒâ€žÃ¢â‚¬ËœÃƒÂ¡Ã‚Â»Ã¢â‚¬Å“ng (62.9) ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ chia PRICE_BOARD_SCALE Ãƒâ€žÃ¢â‚¬ËœÃƒÂ¡Ã‚Â»Ã†â€™ FE merge thÃƒÂ¡Ã‚ÂºÃ‚Â³ng vÃƒÆ’Ã‚Â o nÃƒÂ¡Ã‚ÂºÃ‚Â¿n, khÃƒÆ’Ã‚Â´ng
    quy Ã„â€˜Ã¡Â»â€¢i (xem signal_service). MÃƒÂ£ chÃ†Â°a khÃ¡Â»â€ºp lÃ¡Â»â€¡nh (price <= 0) bÃ¡Â»â€¹ loÃ¡ÂºÂ¡i Ã¢â‚¬â€ FE
    merge low=0 vÃƒÆ’Ã‚Â o nÃƒÂ¡Ã‚ÂºÃ‚Â¿n sÃƒÂ¡Ã‚ÂºÃ‚Â½ sÃƒÂ¡Ã‚ÂºÃ‚Â­p thang giÃƒÆ’Ã‚Â¡. `index_quotes` (Ãƒâ€žÃ¢â‚¬ËœÃƒâ€ Ã‚Â¡n vÃƒÂ¡Ã‚Â»Ã¢â‚¬Â¹ Ãƒâ€žÃ¢â‚¬ËœiÃƒÂ¡Ã‚Â»Ã†â€™m, Ãƒâ€žÃ¢â‚¬ËœÃƒÆ’Ã‚Â£ Ãƒâ€žÃ¢â‚¬ËœÃƒÆ’Ã‚Âºng
    sÃƒÂ¡Ã‚ÂºÃ‚Âµn) gÃƒÂ¡Ã‚Â»Ã¢â€žÂ¢p thÃƒÂ¡Ã‚ÂºÃ‚Â³ng vÃƒÆ’Ã‚Â o data.

    `time` Ã„â€˜Ã¡ÂºÂ·t 1 lÃ¡ÂºÂ§n Ã¡Â»Å¸ ngoÃƒÂ i Ã¢â‚¬â€ cÃ¡ÂºÂ£ snapshot chÃ¡Â»Â¥p cÃƒÂ¹ng thÃ¡Â»Âi Ã„â€˜iÃ¡Â»Æ’m, lÃ¡ÂºÂ·p theo tÃ¡Â»Â«ng mÃƒÂ£
    chÃ¡Â»â€° phÃƒÂ¬nh payload (~1600 mÃƒÂ£); FE ÃƒÂ¡p time nÃƒÂ y cho mÃ¡Â»Âi mÃƒÂ£. `volume` lÃƒÂ  KL khÃ¡Â»â€ºp
    tÃƒÆ’Ã‚Â­ch lÃƒâ€¦Ã‚Â©y phiÃƒÆ’Ã‚Âªn."""
    data = {}
    for r in board:
        price = r["price"]
        if not price or price <= 0:
            continue
        data[r["symbol"]] = {
            "price": price / signal_service.PRICE_BOARD_SCALE,
            "volume": r.get("volume", 0),
        }
    if index_quotes:
        data.update(index_quotes)
    return {
        "time": int(datetime.now(VN_TZ).timestamp()),
        "data": data,
    }


def fetch_quotes_direct():
    """Fallback cho /quotes khi cache chÃƒâ€ Ã‚Â°a warm (vÃƒÆ’Ã‚Â i giÃƒÆ’Ã‚Â¢y Ãƒâ€žÃ¢â‚¬ËœÃƒÂ¡Ã‚ÂºÃ‚Â§u sau boot): 1
    request price_board trÃ¡Â»Â±c tiÃ¡ÂºÂ¿p (+1 history cho index). LÃ¡Â»â€”i Ã¢â€ â€™ {time: None,
    data: {}} (FE poll lÃƒÂ¡Ã‚ÂºÃ‚Â§n sau tÃƒÂ¡Ã‚Â»Ã‚Â± lÃƒÆ’Ã‚Â nh, khÃƒÆ’Ã‚Â´ng 500)."""
    symbols = _all_symbols_today()
    board = data_source.fetch_vn100_board(symbols) if symbols else None
    if not board:
        return {"time": None, "data": {}}
    return _build_quotes(board, _fetch_index_quotes())


def _vn100_view(board):
    """View /vn100 dÃ¡Â»Â±ng tÃ¡Â»Â« board TOÃƒâ‚¬N thÃ¡Â»â€¹ trÃ†Â°Ã¡Â»Âng: lÃ¡Â»Âc vÃ¡Â»Â rÃ¡Â»â€¢ theo dÃƒÂµi rÃ¡Â»â€œi
    build_board (lÃ¡Â»Âc value + sort + gÃ¡ÂºÂ¯n signal). ChÃ¡ÂºÂ¡y mÃ¡Â»â€”i chu kÃ¡Â»Â³ 20s Ã„â€˜Ã¡Â»Æ’ tÃƒÂ­n hiÃ¡Â»â€¡u
    cÃ¡ÂºÂ¯t TRONG PHIÃƒÅ N hiÃ¡Â»â€¡n ngay Ã¢â‚¬â€ trÃ†Â°Ã¡Â»â€ºc Ã„â€˜ÃƒÂ¢y field signal chÃ¡Â»â€° Ã„â€˜Ã†Â°Ã¡Â»Â£c attach lÃƒÂºc dÃ¡Â»Â±ng
    snapshot board_vn100 (~1 tiÃ¡ÂºÂ¿ng/lÃ¡ÂºÂ§n) nÃƒÂªn Ã„â€˜ÃƒÂ´ng cÃ¡Â»Â©ng cÃ¡ÂºÂ£ giÃ¡Â»Â dÃƒÂ¹ giÃƒÂ¡ Ã„â€˜ÃƒÂ£ cÃ¡ÂºÂ¯t
    EMA20/MACD (bug 07/07/2026). KhÃƒÆ’Ã‚Â´ng call vnstock thÃƒÆ’Ã‚Âªm: tÃƒÆ’Ã‚Â¡i dÃƒÆ’Ã‚Â¹ng board cÃƒÂ¡Ã‚Â»Ã‚Â§a
    fetch_market_snapshot. ChÃ†Â°a cÃƒÂ³ danh sÃƒÂ¡ch rÃ¡Â»â€¢ (listing lÃ¡Â»â€”i) Ã¢â€ â€™ None Ã„â€˜Ã¡Â»Æ’ endpoint
    fallback snapshot board_vn100."""
    basket = set(vn100_service.get_symbols())
    if not basket:
        return None
    return vn100_service.build_board([r for r in board if r["symbol"] in basket])


def _preopen_power_view():
    """Power lÃºc boot trÆ°á»›c phiÃªn: dá»±ng láº¡i tá»« snapshot board_vn100 (last-good
    tá»« Ä‘Ä©a, Ä‘Ã£ load á»Ÿ lifespan trÆ°á»›c tick Ä‘áº§u) Ä‘á»ƒ /power tráº£ Ä‘á»“ thá»‹ phiÃªn trÆ°á»›c
    thay vÃ¬ rá»—ng. ChÆ°a tá»«ng cÃ³ snapshot (boot láº§n Ä‘áº§u tiÃªn) â†’ {"data": []}."""
    snap = market_cache.get_snapshot("board_vn100") or {}
    rows = ((snap.get("vn100") or {}).get("data")) or []
    if not rows:
        return {"data": []}
    return vn100_service.power_board_from_vn100_rows(rows)


def refresh_tick(build_views=True):
    """1 request price_board(toÃ n TT) má»—i tick â†’ LUÃ”N cáº­p nháº­t snapshot `quotes`
    (giá + KL khớp real-time cho /quotes, nhịp 1s trong giờ GD).

    build_views=True (má»—i tick thá»© VIEWS_EVERY_TICKS ~20s, lÃºc warm, vÃ  ngoÃ i
    giờ) dựng thêm snapshot `market_wide`: breadth + depth + top-volume + power
    + vn100 (báº£ng giÃ¡ kÃ¨m signal live, xem _vn100_view) â€” táº¥t cáº£ tá»« CÃ™NG má»™t
    láº§n fetch, khÃ´ng call vnstock thÃªm. Fetch lá»—i â†’ Ä‘Ã¡nh dáº¥u stale cáº£ hai,
    giá»¯ báº£n tá»‘t gáº§n nháº¥t (last-good)."""
    symbols = _all_symbols_today()
    if not symbols:
        market_cache.set_snapshot("quotes", None, ok=False)
        if build_views:
            market_cache.set_snapshot("market_wide", None, ok=False)
            market_cache.set_snapshot("market_board_full", None, ok=False)
        return
    snap = data_source.fetch_market_snapshot(symbols)
    if snap is None:
        market_cache.set_snapshot("quotes", None, ok=False)
        if build_views:
            market_cache.set_snapshot("market_wide", None, ok=False)
            market_cache.set_snapshot("market_board_full", None, ok=False)
        return
    market_cache.set_snapshot(
        "quotes", _build_quotes(snap["board"], _fetch_index_quotes())
    )
    if not build_views:
        return
    preopen = not _board_has_trades(snap["board"])
    if preopen and market_cache.get_snapshot("market_wide"):
        # Trước giờ mở phiên: giữ market_wide phiên trước (power/vn100 dựng từ
        # board value=0 sẽ rỗng → trang power + bộ lọc trắng).
        market_cache.set_snapshot("market_wide", None, ok=False)
        market_cache.set_snapshot("market_board_full", None, ok=False)
        return
    volumes = signal_service.volumes_snapshot()
    breadth = homepage_service.build_market_breadth(
        snap["board"], _prev_total_volume_today()
    )
    depth = homepage_service.build_market_depth(snap["bid_ask"])
    if preopen:
        # Boot trÆ°á»›c phiÃªn: láº¥y láº¡i breadth/depth phiÃªn trÆ°á»›c tá»« cache Ä‘Ä©a
        # board_vn100 (đính kèm lúc save hàng giờ) thay vì số 0. Riêng
        # prev_total_volume dÃ¹ng báº£n tÃ­nh má»›i hÃ´m nay (KL "hÃ´m qua" Ä‘Ãºng
        # nghÄ©a â€” báº£n lÆ°u trÃªn Ä‘Ä©a lÃ  cá»§a hÃ´m kia). Cache cÅ© chÆ°a cÃ³ key
        # (file tá»« trÆ°á»›c feature) â†’ giá»¯ báº£n vá»«a dá»±ng.
        saved = market_cache.get_snapshot("board_vn100") or {}
        if saved.get("breadth"):
            breadth = {**saved["breadth"], "prev_total_volume": breadth["prev_total_volume"]}
        depth = saved.get("depth") or depth
    market_cache.set_snapshot(
        "market_wide",
        {
            "breadth": breadth,
            "depth": depth,
            "top_volume": homepage_service.build_top_volume(volumes, snap["bid_ask"], limit=10),
            # Boot trÆ°á»›c phiÃªn (chÆ°a cÃ³ last-good): power dá»±ng tá»« snapshot
            # board_vn100 (last-good tá»« Ä‘Ä©a) Ä‘á»ƒ trang power hiá»‡n Ä‘á»“ thá»‹ phiÃªn
            # trÆ°á»›c; giá»¯ key `power` Ä‘á»ƒ endpoint /power khÃ´ng fan-out fetch
            # theo request. vn100=None Ä‘á»ƒ endpoint /vn100 fallback snapshot
            # board_vn100 trá»±c tiáº¿p.
            "power": _preopen_power_view() if preopen else vn100_service.build_power_board(snap["board"]),
            "vn100": None if preopen else _vn100_view(snap["board"]),
        },
    )
    # Board TOÀN thị trường (chưa lọc value) cho flow_surge_service mở rộng
    # universe â€” 0 call vnstock thÃªm (tÃ¡i dÃ¹ng cÃ¹ng fetch). Preopen (board chÆ°a
    # cÃ³ giao dá»‹ch) â†’ giá»¯ last-good, khÃ´ng ghi Ä‘Ã¨ báº±ng board value=0.
    if not preopen:
        market_cache.set_snapshot("market_board_full", snap["board"])
        _merge_board_session_into_sector_flow_history(snap["board"])
        foreign_trading_service.update_history_from_board(snap["board"])
        global _last_full_board_save_at
        now_mono = time.monotonic()
        if now_mono - _last_full_board_save_at > FULL_BOARD_SAVE_INTERVAL_S:
            _save_market_board_full_cache(snap["board"])
            _last_full_board_save_at = now_mono


def warm_intraday():
    """NÃ¡ÂºÂ¡p nÃ¡ÂºÂ¿n mÃ¡ÂºÂ·c Ã„â€˜Ã¡Â»â€¹nh vÃƒÂ o cache (tÃƒÂ´n trÃ¡Â»Âng TTL + single-flight cÃ¡Â»Â§a market_cache)."""
    for symbol, interval in WARM_INTRADAY:
        market_cache.get_intraday(symbol, interval)


def _run_step(step):
    """ChÃ¡ÂºÂ¡y 1 bÃ†Â°Ã¡Â»â€ºc refresh, nuÃ¡Â»â€˜t mÃ¡Â»Âi lÃ¡Â»â€”i (chÃ¡Â»â€° log) Ã„â€˜Ã¡Â»Æ’ 1 bÃ†Â°Ã¡Â»â€ºc hÃ¡Â»Âng khÃƒÂ´ng kÃƒÂ©o theo
    cÃƒÆ’Ã‚Â¡c bÃƒâ€ Ã‚Â°ÃƒÂ¡Ã‚Â»Ã¢â‚¬Âºc cÃƒÆ’Ã‚Â²n lÃƒÂ¡Ã‚ÂºÃ‚Â¡i vÃƒÆ’Ã‚Â  khÃƒÆ’Ã‚Â´ng giÃƒÂ¡Ã‚ÂºÃ‚Â¿t scheduler_loop."""
    try:
        step()
    except BaseException as e:  # noqa: BLE001 Ã¢â‚¬â€ cÃ¡Â»â€˜ ÃƒÂ½ bÃ¡ÂºÂ¯t cÃ¡ÂºÂ£ SystemExit
        logger.warning("refresh step %s thÃƒÂ¡Ã‚ÂºÃ‚Â¥t bÃƒÂ¡Ã‚ÂºÃ‚Â¡i: %s", step.__name__, e, exc_info=True)


def refresh_all():
    """MÃƒÂ¡Ã‚Â»Ã¢â€žÂ¢t lÃƒâ€ Ã‚Â°ÃƒÂ¡Ã‚Â»Ã‚Â£t refresh Ãƒâ€žÃ¢â‚¬ËœÃƒÂ¡Ã‚ÂºÃ‚Â§y Ãƒâ€žÃ¢â‚¬ËœÃƒÂ¡Ã‚Â»Ã‚Â§. KhÃƒÆ’Ã‚Â³a chÃƒÂ¡Ã‚Â»Ã¢â‚¬Ëœng chÃƒÂ¡Ã‚ÂºÃ‚Â¡y chÃƒÂ¡Ã‚Â»Ã¢â‚¬Å“ng (giÃƒÂ¡Ã‚Â»Ã¢â‚¬Ëœng signal_service).

    MÃ¡Â»â€”i bÃ†Â°Ã¡Â»â€ºc cÃƒÂ´ lÃ¡ÂºÂ­p: board_vn100 lÃ¡Â»â€”i vÃ¡ÂºÂ«n KHÃƒâ€NG chÃ¡ÂºÂ·n market_wide / warm_intraday,
    vÃƒÂ  khÃƒÂ´ng lÃƒÂ m vÃ„Æ’ng lÃ¡Â»â€”i ra scheduler_loop (trÃƒÂ¡nh chÃ¡ÂºÂ¿t luÃ¡Â»â€œng nÃ¡Â»Ân vÃ„Â©nh viÃ¡Â»â€¦n).

    Board VN100 chÃƒÂ¡Ã‚Â»Ã¢â‚¬Â° refresh khi tÃƒÂ¡Ã‚Â»Ã¢â‚¬Âºi hÃƒÂ¡Ã‚ÂºÃ‚Â¡n ~1 tiÃƒÂ¡Ã‚ÂºÃ‚Â¿ng (_maybe_refresh_board_vn100);
    quotes + market_wide + warm nÃ¡ÂºÂ¿n vÃ¡ÂºÂ«n chÃ¡ÂºÂ¡y mÃ¡Â»â€”i lÃ†Â°Ã¡Â»Â£t."""
    if not _refresh_lock.acquire(blocking=False):
        logger.info("market refresh Ã„â€˜ang chÃ¡ÂºÂ¡y Ã¢â‚¬â€ bÃ¡Â»Â qua lÃ¡ÂºÂ§n gÃ¡Â»Âi chÃ¡Â»â€œng")
        return
    try:
        _run_step(_maybe_refresh_board_vn100)
        _run_step(refresh_tick)
        _run_step(_maybe_refresh_index_points)
        _run_step(_maybe_refresh_put_through)
        _run_step(warm_intraday)
        # Đặt CUỐI: bước này nặng (~4 phút), chạy trước sẽ trì hoãn mọi snapshot
        # cÃ²n láº¡i á»Ÿ lÆ°á»£t refresh Ä‘áº§u tiÃªn sau khi khá»Ÿi Ä‘á»™ng.
        _run_step(_maybe_refresh_sector_flow)
        # Buoc nay chi SPAWN luong nen roi tra ve ngay (xem ham) — dat cuoi cho
        # cung nhom "viec nang", nhung no khong keo dai luot refresh nay.
        _run_step(_maybe_backfill_foreign_history)
    finally:
        _refresh_lock.release()


def _quotes_tick():
    refresh_tick(build_views=False)


def refresh_quotes_only():
    """Tick nhanh 1s trong giờ GD: chỉ cập nhật snapshot quotes (1 call
    price_board). DÃ¹ng chung khÃ³a vá»›i refresh_all Ä‘á»ƒ khÃ´ng fetch chá»“ng."""
    if not _refresh_lock.acquire(blocking=False):
        logger.info("market refresh Ã„â€˜ang chÃ¡ÂºÂ¡y Ã¢â‚¬â€ bÃ¡Â»Â qua tick quotes")
        return
    try:
        _run_step(_quotes_tick)
    finally:
        _refresh_lock.release()


async def scheduler_loop():
    """Warm ngay lúc khởi động rồi chạy tick: trong giờ GD ngủ 1s/tick — tick
    thường chỉ cập nhật quotes, mỗi tick thứ VIEWS_EVERY_TICKS (~20s) chạy
    refresh_all (views + warm nến + board_vn100 tới hạn); ngoài giờ giãn 300s,
    má»—i lÆ°á»£t cháº¡y full refresh_all (quotes váº«n Ä‘Æ°á»£c cáº­p nháº­t kÃ¨m â€” giÃ¡ Ä‘Ã³ng
    cá»­a, khÃ´ng cáº§n nhá»‹p nhanh)."""
    await asyncio.to_thread(refresh_all)
    tick = 0
    while True:
        if is_market_hours():
            await asyncio.sleep(QUOTES_INTERVAL_S)
            tick += 1
            if tick % VIEWS_EVERY_TICKS == 0:
                await asyncio.to_thread(refresh_all)
            else:
                await asyncio.to_thread(refresh_quotes_only)
        else:
            tick = 0
            await asyncio.sleep(IDLE_INTERVAL_S)
            await asyncio.to_thread(refresh_all)





