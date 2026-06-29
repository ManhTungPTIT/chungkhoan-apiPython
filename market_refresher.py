"""Luồng nền refresh các snapshot dùng chung vào market_cache.

CHỈ luồng này gọi vnstock cho dữ liệu chung → số call không phụ thuộc số user.
Chạy qua asyncio.to_thread (vnstock là call đồng bộ) để không chặn event loop,
theo đúng pattern signal_service.scheduler_loop.

Ngân sách mỗi chu kỳ (giờ GD): 1 price_board(VN100) + 1 price_board(toàn TT) +
nến intraday mặc định ⇒ ~3 call/chu kỳ × 3 chu kỳ/phút ≈ 9 call/phút (dưới 60).
"""

import asyncio
import logging
import threading
from datetime import datetime, timedelta, timezone

import data_source
import homepage_service
import market_cache
import sector_service
import signal_service
import vn100_service

logger = logging.getLogger(__name__)

VN_TZ = timezone(timedelta(hours=7))

REALTIME_INTERVAL_S = 20   # trong giờ giao dịch
IDLE_INTERVAL_S = 300      # ngoài giờ: vẫn refresh thưa để có giá đóng cửa mới nhất
MARKET_OPEN_HOUR = 9
MARKET_CLOSE_HOUR = 15     # tới 15:00 (giờ VN)

# Nến dựng sẵn cho trang mặc định (FE mở VNINDEX khung 1d khi khởi động).
WARM_INTRADAY = [("VNINDEX", "1d")]

_refresh_lock = threading.Lock()
_prev_volume_date = None   # ngày đã tính prev_total_volume (chỉ đổi theo ngày)
_prev_total_volume = 0


def is_market_hours(now=None):
    """T2–T6, 09:00 ≤ giờ < 15:00 (giờ VN). Ngoài khoảng này coi như ngoài phiên."""
    now = now or datetime.now(VN_TZ)
    if now.weekday() >= 5:  # 5=T7, 6=CN
        return False
    return MARKET_OPEN_HOUR <= now.hour < MARKET_CLOSE_HOUR


def _prev_total_volume_today():
    """KL khớp phiên hôm qua — chỉ đổi theo ngày nên tính 1 lần/ngày (3 history call)."""
    global _prev_volume_date, _prev_total_volume
    today = datetime.now(VN_TZ).strftime("%Y-%m-%d")
    if _prev_volume_date != today:
        _prev_total_volume = homepage_service.prev_total_volume()
        _prev_volume_date = today
    return _prev_total_volume


def refresh_board_vn100():
    """1 request price_board(VN100) → dựng sẵn 4 view: vn100, sectors, heatmap, groups.

    Fetch lỗi → đánh dấu stale, GIỮ snapshot cũ (last-good)."""
    symbols = vn100_service.get_symbols()
    if not symbols:
        market_cache.set_snapshot("board_vn100", None, ok=False)
        return
    board = data_source.fetch_vn100_board(symbols)
    if board is None:
        market_cache.set_snapshot("board_vn100", None, ok=False)
        return
    groups = sector_service.build_groups(board)
    market_cache.set_snapshot(
        "board_vn100",
        {
            "vn100": vn100_service.build_board(board),
            "sectors": sector_service.sectors_view(groups),
            "heatmap": sector_service.heatmap_view(groups),
            "groups": groups,  # để tra /sectors/symbols theo icb_code
        },
    )


def refresh_market_wide():
    """1 request price_board(toàn TT) → breadth + depth + top-volume (homepage)."""
    symbols = data_source.fetch_all_symbols()
    if not symbols:
        market_cache.set_snapshot("market_wide", None, ok=False)
        return
    snap = data_source.fetch_market_snapshot(symbols)
    if snap is None:
        market_cache.set_snapshot("market_wide", None, ok=False)
        return
    volumes = signal_service.volumes_snapshot()
    market_cache.set_snapshot(
        "market_wide",
        {
            "breadth": homepage_service.build_market_breadth(
                snap["board"], _prev_total_volume_today()
            ),
            "depth": homepage_service.build_market_depth(snap["bid_ask"]),
            "top_volume": homepage_service.build_top_volume(volumes, snap["bid_ask"], limit=10),
        },
    )


def warm_intraday():
    """Nạp nến mặc định vào cache (tôn trọng TTL + single-flight của market_cache)."""
    for symbol, interval in WARM_INTRADAY:
        market_cache.get_intraday(symbol, interval)


def refresh_all():
    """Một lượt refresh đầy đủ. Khóa chống chạy chồng (giống signal_service)."""
    if not _refresh_lock.acquire(blocking=False):
        logger.info("market refresh đang chạy — bỏ qua lần gọi chồng")
        return
    try:
        refresh_board_vn100()
        refresh_market_wide()
        warm_intraday()
    finally:
        _refresh_lock.release()


async def scheduler_loop():
    """Warm ngay lúc khởi động rồi refresh theo chu kỳ; giãn nhịp ngoài giờ GD."""
    await asyncio.to_thread(refresh_all)
    while True:
        interval = REALTIME_INTERVAL_S if is_market_hours() else IDLE_INTERVAL_S
        await asyncio.sleep(interval)
        await asyncio.to_thread(refresh_all)
