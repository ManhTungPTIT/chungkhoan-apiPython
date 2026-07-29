"""Luồng nền refresh các snapshot dùng chung vào market_cache.

CHỈ luồng này gọi vnstock cho dữ liệu chung → số call không phụ thuộc số user.
Chạy qua asyncio.to_thread (vnstock là call đồng bộ) để không chặn event loop,
theo đúng pattern signal_service.scheduler_loop.

Ngân sách trong giờ GD: tick 1s × (4 price_board — chia theo sàn HSX/HNX/UPCOM,
UPCOM ~900 mã chia tiếp 2 mẻ ≤500 mã theo khuyến nghị vendor (xem
data_source.PRICE_BOARD_CHUNK_MAX) + 1 history nến 1D VNINDEX cho /quotes)
= 300 call/phút — tick thường chỉ cập nhật snapshot quotes (/quotes); mỗi
tick thứ 20 (~20s) tái dùng CÙNG lần fetch đó dựng thêm views market_wide +
warm nến VNINDEX (3 call/phút, KHÔNG đổi so với bản 5s trước — vẫn giữ nhịp
20s). Tổng ~303 call/phút — dưới hạn Golden 500 req/phút, nhưng VƯỢT hạn
Community 60 req/phút (chấp nhận được vì project chạy tier Golden, xem
vnstock_license; nếu rớt về Community phải tăng lại QUOTES_INTERVAL_S).
Danh sách mã (all_symbols) memoize theo ngày, không tính vào ngân sách trên.
Board VN100 (price_board rổ VNALL+HNX) KHÔNG chạy mỗi chu kỳ mà giãn ~1 tiếng/lần
(BOARD_VN100_INTERVAL_S) — sectors + heatmap đổi chậm, không cần 20s. Riêng view
/vn100 (bảng giá + signal) được dựng lại mỗi ~20s từ board toàn TT của
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
from app.data import market_cache
from app.services import sector_service
from app.services import signal_service
from app.services import vn100_service

logger = logging.getLogger(__name__)

VN_TZ = timezone(timedelta(hours=7))

QUOTES_INTERVAL_S = 5      # tick nhanh trong giờ GD: cập nhật snapshot quotes
VIEWS_EVERY_TICKS = 20     # mỗi tick thứ 20 (20×1s = 20s) dựng thêm views nặng
REALTIME_INTERVAL_S = QUOTES_INTERVAL_S * VIEWS_EVERY_TICKS  # nhịp views (giữ ~20s như cũ)
IDLE_INTERVAL_S = 300      # ngoÃƒÂ i giÃ¡Â»Â: vÃ¡ÂºÂ«n refresh thÃ†Â°a Ã„â€˜Ã¡Â»Æ’ cÃƒÂ³ giÃƒÂ¡ Ã„â€˜ÃƒÂ³ng cÃ¡Â»Â­a mÃ¡Â»â€ºi nhÃ¡ÂºÂ¥t
MARKET_OPEN_HOUR = 9
MARKET_CLOSE_HOUR = 15     # tÃ¡Â»â€ºi 15:00 (giÃ¡Â»Â VN)

# Danh sÃƒÂ¡ch mÃƒÂ£ toÃƒÂ n TT memoize theo NGÃƒâ‚¬Y: listing chÃ¡Â»â€° Ã„â€˜Ã¡Â»â€¢i khi cÃƒÂ³ niÃƒÂªm yÃ¡ÂºÂ¿t mÃ¡Â»â€ºi
# (rÃ¡ÂºÂ¥t hiÃ¡ÂºÂ¿m), trÃ†Â°Ã¡Â»â€ºc Ã„â€˜ÃƒÂ¢y fetch mÃ¡Â»â€”i chu kÃ¡Â»Â³ 20s = 3 call/phÃƒÂºt vÃƒÂ´ ÃƒÂ­ch.
_all_symbols: list[str] = []
_all_symbols_date = None   # ngÃƒÂ y (giÃ¡Â»Â VN) Ã„â€˜ÃƒÂ£ fetch danh sÃƒÂ¡ch thÃƒÂ nh cÃƒÂ´ng

# Board VN100 (price_board rÃ¡Â»â€¢ VNALL+HNX Ã¢â€ â€™ bÃ¡ÂºÂ£ng giÃƒÂ¡/sectors/heatmap) refresh giÃƒÂ£n
# ~1 tiÃ¡ÂºÂ¿ng/lÃ¡ÂºÂ§n thay vÃƒÂ¬ mÃ¡Â»â€”i chu kÃ¡Â»Â³ 20s: dÃ¡Â»Â¯ liÃ¡Â»â€¡u Ã„â€˜Ã¡Â»â€¢i chÃ¡ÂºÂ­m, tiÃ¡ÂºÂ¿t kiÃ¡Â»â€¡m ~3 call/phÃƒÂºt.
BOARD_VN100_INTERVAL_S = 3600
_last_board_vn100_at = 0.0  # time.monotonic() lÃ¡ÂºÂ§n refresh board VN100 gÃ¡ÂºÂ§n nhÃ¡ÂºÂ¥t (thÃƒÂ nh cÃƒÂ´ng)
# Bảng thỏa thuận: 3 request/lượt (HOSE+HNX+UPCOM). Lệnh thỏa thuận thưa (vài
# chục lệnh/phiên) nên 60s là quá đủ, không cần bám nhịp views 20s.
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
# NÃ¡ÂºÂ¿n dÃ¡Â»Â±ng sÃ¡ÂºÂµn cho trang mÃ¡ÂºÂ·c Ã„â€˜Ã¡Â»â€¹nh (FE mÃ¡Â»Å¸ VNINDEX khung 1d khi khÃ¡Â»Å¸i Ã„â€˜Ã¡Â»â„¢ng).
WARM_INTRADAY = [("VNINDEX", "1d")]

_refresh_lock = threading.Lock()
_prev_volume_date = None   # ngÃƒÂ y Ã„â€˜ÃƒÂ£ tÃƒÂ­nh prev_total_volume (chÃ¡Â»â€° Ã„â€˜Ã¡Â»â€¢i theo ngÃƒÂ y)
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
    heatmap) dựng từ board này sẽ rỗng/0 → phải coi là stale để giữ dữ liệu
    PHIÊN TRƯỚC (last-good) thay vì ghi đè bằng bản trắng (bug trắng trang
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
        # Chưa từng có dữ liệu (boot lần đầu trước phiên): vẫn dựng khung snapshot
        # để /sectors, /heatmap có nhóm ngành thay vì trống hẳn, nhưng KHÔNG ghi
        # đĩa và trả False để không dời mốc ~1 tiếng — chu kỳ sau thử lại, vào
        # phiên là có dữ liệu thật ngay.
        market_cache.set_snapshot("board_vn100", _build_board_vn100_snapshot(board))
        return False
    snapshot = _build_board_vn100_snapshot(board)
    # Đính kèm breadth/depth (đã dựng sẵn trong market_wide mỗi ~20s — 0 call
    # thêm) vào cache đĩa: boot trước phiên đọc lại để /homepage/market-breadth
    # và market-depth hiện số phiên trước thay vì 0 (cùng tinh thần power).
    # Chưa có market_wide (boot đầu phiên, bước này chạy trước refresh_tick
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
# cả sau restart qua đêm — RAM last-good mất khi restart. Ghi throttled (không
# mỗi tick ~20s) vì payload full board lớn hơn board_vn100.
MARKET_BOARD_FULL_CACHE_FILE = os.environ.get(
    "MARKET_BOARD_FULL_CACHE_FILE",
    os.path.join(RUNTIME_DATA_DIR, "market_board_full_cache.json"),
)
FULL_BOARD_SAVE_INTERVAL_S = 600  # ghi đĩa tối đa mỗi ~10 phút
_last_full_board_save_at = 0.0


def _save_market_board_full_cache(rows):
    try:
        with open(MARKET_BOARD_FULL_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False)
    except OSError as e:
        logger.warning("ghi market_board_full cache that bai: %s", e)


def load_market_board_full_cache():
    """Nạp last-good board toàn TT từ đĩa trước lần fetch vnstock đầu — để
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
# (16 luồng chỉ nhanh hơn ~10%, không đáng đánh đổi rủi ro bị chặn).
SECTOR_FLOW_WORKERS = 8
# Chỉ cần ~5 phiên gần nhất; lấy dư 20 ngày lịch để chắc chắn vượt cuối tuần và
# nghỉ lễ dài.
SECTOR_FLOW_LOOKBACK_DAYS = 20
_sector_flow_date = None  # ngày (giờ VN) đã nạp history toàn thị trường xong


def _finite(value):
    """Chuỗi vendor → float, None nếu không phải số hữu hạn ('nan', '', None)."""
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _normalize_candles(rows):
    """Nến ngày rút gọn cho snapshot `sector_flow_history`, close/volume ép về SỐ.

    `data_source._map_history` ép cả DataFrame về chuỗi (`df.astype(str)`) nên
    vendor trả close "25.5" / volume "1101". Consumer nhân thẳng close × volume
    (index_overview_service) sẽ nổ TypeError. Ép ngay tại chỗ sinh snapshot —
    đúng cách signal_service làm khi dựng `_history_candles` — để không service
    nào phải tự nhớ ép kiểu.

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
        logger.warning("ghi sector flow cache thất bại: %s", e)


def load_sector_flow_cache():
    """Nạp lại history 5 phiên từ đĩa lúc khởi động — nếu không, mỗi lần restart
    lại phải quét ~1.600 mã (~4 phút) trước khi hai chart 5 phiên có dữ liệu."""
    global _sector_flow_date
    try:
        with open(SECTOR_FLOW_CACHE_FILE, encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, ValueError):
        return None
    raw = payload.get("history") if isinstance(payload, dict) else None
    if not raw:
        return None
    # Cache ghi trước bản vá kiểu dữ liệu vẫn còn chuỗi, mà restart thì nạp thẳng
    # từ đĩa chứ không chạy lại refresh — không chuẩn hoá ở đây là chart hỏng tới
    # tận lượt quét ngày hôm sau.
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


def refresh_sector_flow():
    """Nến ngày TOÀN thị trường cho hai chart "5 phiên gần nhất".

    Đây là luồng nặng nhất của hệ thống (~1.600 request history) nên chỉ chạy MỘT
    LẦN MỖI NGÀY và fetch song song. Mã lỗi lẻ tẻ được bỏ qua chứ không retry:
    thiếu vài mã chỉ làm lệch nhẹ tổng ngành, còn quét lại cả rổ thì tốn thêm vài
    phút. Ghi ra đĩa để restart không phải quét lại.
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
    _save_sector_flow_cache({"date": today.isoformat(), "history": history})
    logger.info("sector flow history: %d/%d mã", len(history), len(symbols))
    return True


def _maybe_refresh_sector_flow(now=None):
    """Chạy tối đa 1 lần/ngày. Chỉ dời mốc khi THÀNH CÔNG → hôm nào lỗi thì lượt
    refresh sau thử lại thay vì đợi sang ngày mới."""
    global _sector_flow_date
    today = (now or datetime.now(VN_TZ)).date().isoformat()
    if _sector_flow_date == today:
        return
    if refresh_sector_flow():
        _sector_flow_date = today


PUT_THROUGH_CACHE_FILE = os.environ.get(
    "PUT_THROUGH_CACHE_FILE",
    os.path.join(RUNTIME_DATA_DIR, "put_through_cache.json"),
)


def _save_put_through_cache(deals):
    try:
        with open(PUT_THROUGH_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(deals, f, ensure_ascii=False)
    except OSError as e:
        logger.warning("ghi put_through cache thất bại: %s", e)


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
    ngưỡng) để endpoint đổi min_value không phải fetch lại.

    Hai trường hợp đều GIỮ bản tốt gần nhất, vì cùng một lý do — giá trị thỏa
    thuận chỉ tăng dần trong phiên nên bản cũ vẫn đọc được, còn ghi đè bằng rỗng
    thì biểu đồ trắng:
      - fetch hỏng cả 3 sàn (None) → đánh dấu stale;
      - vendor trả RỖNG hợp lệ (sáng sớm chưa ai giao dịch thỏa thuận) → im lặng
        giữ nguyên. Lệnh đầu tiên của phiên mới sẽ tự ghi đè.
    Chưa từng có dữ liệu thì [] mới được ghi vào, để chart hiện trạng thái rỗng
    thay vì treo mãi ở "đang tải".
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
    refresh THÀNH CÔNG → lần lỗi được thử lại ở lượt kế (self-heal)."""
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


# Index cÃ¡ÂºÂ§n giÃƒÂ¡ realtime trong /quotes (mÃƒÂ n hÃƒÂ¬nh mÃ¡ÂºÂ·c Ã„â€˜Ã¡Â»â€¹nh FE mÃ¡Â»Å¸ VNINDEX). Index
# khÃƒÂ´ng cÃƒÂ³ trong price_board (board chÃ¡Â»â€° cÃ¡Â»â€¢ phiÃ¡ÂºÂ¿u) Ã¢â€ â€™ fetch riÃƒÂªng qua history.
INDEX_QUOTE_SYMBOLS = ["VNINDEX"]


def _fetch_index_quotes():
    """GiÃƒÂ¡ index cho /quotes: nÃ¡ÂºÂ¿n 1D cÃ¡Â»Â§a RIÃƒÅ NG hÃƒÂ´m nay (1 call/index, trÃ¡ÂºÂ£ 1 nÃ¡ÂºÂ¿n)
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

    GiÃƒÂ¡ cÃ¡Â»â€¢ phiÃ¡ÂºÂ¿u tÃ¡Â»Â« price_board lÃƒÂ  VND thÃƒÂ´ (vd 62900) cÃƒÂ²n nÃ¡ÂºÂ¿n /intraday theo
    nghÃƒÂ¬n Ã„â€˜Ã¡Â»â€œng (62.9) Ã¢â€ â€™ chia PRICE_BOARD_SCALE Ã„â€˜Ã¡Â»Æ’ FE merge thÃ¡ÂºÂ³ng vÃƒÂ o nÃ¡ÂºÂ¿n, khÃƒÂ´ng
    quy Ã„â€˜Ã¡Â»â€¢i (xem signal_service). MÃƒÂ£ chÃ†Â°a khÃ¡Â»â€ºp lÃ¡Â»â€¡nh (price <= 0) bÃ¡Â»â€¹ loÃ¡ÂºÂ¡i Ã¢â‚¬â€ FE
    merge low=0 vÃƒÂ o nÃ¡ÂºÂ¿n sÃ¡ÂºÂ½ sÃ¡ÂºÂ­p thang giÃƒÂ¡. `index_quotes` (Ã„â€˜Ã†Â¡n vÃ¡Â»â€¹ Ã„â€˜iÃ¡Â»Æ’m, Ã„â€˜ÃƒÂ£ Ã„â€˜ÃƒÂºng
    sÃ¡ÂºÂµn) gÃ¡Â»â„¢p thÃ¡ÂºÂ³ng vÃƒÂ o data.

    `time` Ã„â€˜Ã¡ÂºÂ·t 1 lÃ¡ÂºÂ§n Ã¡Â»Å¸ ngoÃƒÂ i Ã¢â‚¬â€ cÃ¡ÂºÂ£ snapshot chÃ¡Â»Â¥p cÃƒÂ¹ng thÃ¡Â»Âi Ã„â€˜iÃ¡Â»Æ’m, lÃ¡ÂºÂ·p theo tÃ¡Â»Â«ng mÃƒÂ£
    chÃ¡Â»â€° phÃƒÂ¬nh payload (~1600 mÃƒÂ£); FE ÃƒÂ¡p time nÃƒÂ y cho mÃ¡Â»Âi mÃƒÂ£. `volume` lÃƒÂ  KL khÃ¡Â»â€ºp
    tÃƒÂ­ch lÃ…Â©y phiÃƒÂªn."""
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
    """Fallback cho /quotes khi cache chÃ†Â°a warm (vÃƒÂ i giÃƒÂ¢y Ã„â€˜Ã¡ÂºÂ§u sau boot): 1
    request price_board trÃ¡Â»Â±c tiÃ¡ÂºÂ¿p (+1 history cho index). LÃ¡Â»â€”i Ã¢â€ â€™ {time: None,
    data: {}} (FE poll lÃ¡ÂºÂ§n sau tÃ¡Â»Â± lÃƒÂ nh, khÃƒÂ´ng 500)."""
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
    EMA20/MACD (bug 07/07/2026). KhÃƒÂ´ng call vnstock thÃƒÂªm: tÃƒÂ¡i dÃƒÂ¹ng board cÃ¡Â»Â§a
    fetch_market_snapshot. ChÃ†Â°a cÃƒÂ³ danh sÃƒÂ¡ch rÃ¡Â»â€¢ (listing lÃ¡Â»â€”i) Ã¢â€ â€™ None Ã„â€˜Ã¡Â»Æ’ endpoint
    fallback snapshot board_vn100."""
    basket = set(vn100_service.get_symbols())
    if not basket:
        return None
    return vn100_service.build_board([r for r in board if r["symbol"] in basket])


def _preopen_power_view():
    """Power lúc boot trước phiên: dựng lại từ snapshot board_vn100 (last-good
    từ đĩa, đã load ở lifespan trước tick đầu) để /power trả đồ thị phiên trước
    thay vì rỗng. Chưa từng có snapshot (boot lần đầu tiên) → {"data": []}."""
    snap = market_cache.get_snapshot("board_vn100") or {}
    rows = ((snap.get("vn100") or {}).get("data")) or []
    if not rows:
        return {"data": []}
    return vn100_service.power_board_from_vn100_rows(rows)


def refresh_tick(build_views=True):
    """1 request price_board(toàn TT) mỗi tick → LUÔN cập nhật snapshot `quotes`
    (giá + KL khớp real-time cho /quotes, nhịp 1s trong giờ GD).

    build_views=True (mỗi tick thứ VIEWS_EVERY_TICKS ~20s, lúc warm, và ngoài
    giờ) dựng thêm snapshot `market_wide`: breadth + depth + top-volume + power
    + vn100 (bảng giá kèm signal live, xem _vn100_view) — tất cả từ CÙNG một
    lần fetch, không call vnstock thêm. Fetch lỗi → đánh dấu stale cả hai,
    giữ bản tốt gần nhất (last-good)."""
    symbols = _all_symbols_today()
    if not symbols:
        market_cache.set_snapshot("quotes", None, ok=False)
        if build_views:
            market_cache.set_snapshot("market_wide", None, ok=False)
        return
    snap = data_source.fetch_market_snapshot(symbols)
    if snap is None:
        market_cache.set_snapshot("quotes", None, ok=False)
        if build_views:
            market_cache.set_snapshot("market_wide", None, ok=False)
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
        return
    volumes = signal_service.volumes_snapshot()
    breadth = homepage_service.build_market_breadth(
        snap["board"], _prev_total_volume_today()
    )
    depth = homepage_service.build_market_depth(snap["bid_ask"])
    if preopen:
        # Boot trước phiên: lấy lại breadth/depth phiên trước từ cache đĩa
        # board_vn100 (đính kèm lúc save hàng giờ) thay vì số 0. Riêng
        # prev_total_volume dùng bản tính mới hôm nay (KL "hôm qua" đúng
        # nghĩa — bản lưu trên đĩa là của hôm kia). Cache cũ chưa có key
        # (file từ trước feature) → giữ bản vừa dựng.
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
            # Boot trước phiên (chưa có last-good): power dựng từ snapshot
            # board_vn100 (last-good từ đĩa) để trang power hiện đồ thị phiên
            # trước; giữ key `power` để endpoint /power không fan-out fetch
            # theo request. vn100=None để endpoint /vn100 fallback snapshot
            # board_vn100 trực tiếp.
            "power": _preopen_power_view() if preopen else vn100_service.build_power_board(snap["board"]),
            "vn100": None if preopen else _vn100_view(snap["board"]),
        },
    )
    # Board TOÀN thị trường (chưa lọc value) cho flow_surge_service mở rộng
    # universe — 0 call vnstock thêm (tái dùng cùng fetch). Preopen (board chưa
    # có giao dịch) → giữ last-good, không ghi đè bằng board value=0.
    if not preopen:
        market_cache.set_snapshot("market_board_full", snap["board"])
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
    cÃƒÂ¡c bÃ†Â°Ã¡Â»â€ºc cÃƒÂ²n lÃ¡ÂºÂ¡i vÃƒÂ  khÃƒÂ´ng giÃ¡ÂºÂ¿t scheduler_loop."""
    try:
        step()
    except BaseException as e:  # noqa: BLE001 Ã¢â‚¬â€ cÃ¡Â»â€˜ ÃƒÂ½ bÃ¡ÂºÂ¯t cÃ¡ÂºÂ£ SystemExit
        logger.warning("refresh step %s thÃ¡ÂºÂ¥t bÃ¡ÂºÂ¡i: %s", step.__name__, e, exc_info=True)


def refresh_all():
    """MÃ¡Â»â„¢t lÃ†Â°Ã¡Â»Â£t refresh Ã„â€˜Ã¡ÂºÂ§y Ã„â€˜Ã¡Â»Â§. KhÃƒÂ³a chÃ¡Â»â€˜ng chÃ¡ÂºÂ¡y chÃ¡Â»â€œng (giÃ¡Â»â€˜ng signal_service).

    MÃ¡Â»â€”i bÃ†Â°Ã¡Â»â€ºc cÃƒÂ´ lÃ¡ÂºÂ­p: board_vn100 lÃ¡Â»â€”i vÃ¡ÂºÂ«n KHÃƒâ€NG chÃ¡ÂºÂ·n market_wide / warm_intraday,
    vÃƒÂ  khÃƒÂ´ng lÃƒÂ m vÃ„Æ’ng lÃ¡Â»â€”i ra scheduler_loop (trÃƒÂ¡nh chÃ¡ÂºÂ¿t luÃ¡Â»â€œng nÃ¡Â»Ân vÃ„Â©nh viÃ¡Â»â€¦n).

    Board VN100 chÃ¡Â»â€° refresh khi tÃ¡Â»â€ºi hÃ¡ÂºÂ¡n ~1 tiÃ¡ÂºÂ¿ng (_maybe_refresh_board_vn100);
    quotes + market_wide + warm nÃ¡ÂºÂ¿n vÃ¡ÂºÂ«n chÃ¡ÂºÂ¡y mÃ¡Â»â€”i lÃ†Â°Ã¡Â»Â£t."""
    if not _refresh_lock.acquire(blocking=False):
        logger.info("market refresh Ã„â€˜ang chÃ¡ÂºÂ¡y Ã¢â‚¬â€ bÃ¡Â»Â qua lÃ¡ÂºÂ§n gÃ¡Â»Âi chÃ¡Â»â€œng")
        return
    try:
        _run_step(_maybe_refresh_board_vn100)
        _run_step(refresh_tick)
        _run_step(_maybe_refresh_put_through)
        _run_step(warm_intraday)
        # Đặt CUỐI: bước này nặng (~4 phút), chạy trước sẽ trì hoãn mọi snapshot
        # còn lại ở lượt refresh đầu tiên sau khi khởi động.
        _run_step(_maybe_refresh_sector_flow)
    finally:
        _refresh_lock.release()


def _quotes_tick():
    refresh_tick(build_views=False)


def refresh_quotes_only():
    """Tick nhanh 1s trong giờ GD: chỉ cập nhật snapshot quotes (1 call
    price_board). Dùng chung khóa với refresh_all để không fetch chồng."""
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
    mỗi lượt chạy full refresh_all (quotes vẫn được cập nhật kèm — giá đóng
    cửa, không cần nhịp nhanh)."""
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


