"""Phục vụ nhóm ngành hay giao dịch — gom rổ VN100 theo ICB cấp 3.

Hai cách dùng, cùng một nguồn dữ liệu (1 request price_board + bản đồ ngành):
- get_sectors(): danh sách nhóm ngành xếp theo tổng giá trị giao dịch giảm dần.
- get_sector_symbols(icb_code): các mã + OHLC phiên hôm nay của một nhóm.

Bản đồ ngành (mã → ICB cấp 3) đổi rất hiếm nên được memoize ở biến module-level,
giống danh sách VN100 trong vn100_service. Fetch hỏng (None) → trả {data: []}.
"""

import json
import logging
import os

from app.data import data_source
from app.services import vn100_service

logger = logging.getLogger(__name__)

UNCLASSIFIED = "Chưa phân loại"

_industry_map: dict = {}

# Bản đồ ngành đổi rất hiếm nhưng lại là thứ MỌI chart theo ngành phụ thuộc vào.
# Chỉ memoize RAM thì sau mỗi lần restart phải fetch lại; fetch hỏng (vendor
# chặn, mất mạng, chưa có license) là mọi mã rơi vào "Chưa phân loại" và các
# chart theo ngành thu về đúng một hàng. Lưu đĩa để boot ngoài giờ vẫn đủ ngành.
INDUSTRY_MAP_CACHE_FILE = os.environ.get(
    "INDUSTRY_MAP_CACHE_FILE",
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "data",
        "industry_map_cache.json",
    ),
)


def _save_industry_map_cache(imap: dict) -> None:
    try:
        with open(INDUSTRY_MAP_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(imap, f, ensure_ascii=False)
    except OSError as e:
        logger.warning("ghi industry map cache thất bại: %s", e)


def load_industry_map_cache():
    """Nạp bản đồ ngành từ đĩa vào memo (gọi lúc khởi động)."""
    global _industry_map
    try:
        with open(INDUSTRY_MAP_CACHE_FILE, encoding="utf-8") as f:
            imap = json.load(f)
    except (OSError, ValueError):
        return None
    if not imap:
        return None
    _industry_map = imap
    return imap


def get_industry_map(fetch_fn=data_source.fetch_industry_map) -> dict:
    """Lấy bản đồ mã → nhóm ICB cấp 3 1 lần rồi memoize (+ lưu đĩa).

    Fetch hỏng (None/rỗng) → giữ nguyên bản đang có để lần gọi sau thử lại
    (self-heal); KHÔNG ghi đè cache tốt trên đĩa bằng bản rỗng.
    """
    global _industry_map
    if not _industry_map:
        fetched = fetch_fn()
        if fetched:
            _industry_map = fetched
            _save_industry_map_cache(fetched)
    return _industry_map


def _build_groups(board: list[dict], imap: dict) -> list[dict]:
    """Gom board theo nhóm ICB cấp 3, xếp nhóm và mã theo value giảm dần."""
    groups: dict[str, dict] = {}
    for item in board:
        info = imap.get(item["symbol"]) or {}
        icb_code = info.get("icb_code", "")
        group = groups.get(icb_code)
        if group is None:
            group = {
                "group": info.get("icb_name", UNCLASSIFIED),
                "icb_code": icb_code,
                "total_value": 0,
                "symbols": [],
            }
            groups[icb_code] = group
        group["total_value"] += item.get("value") or 0
        group["symbols"].append(item)

    for group in groups.values():
        group["symbols"].sort(key=lambda x: x.get("value") or 0, reverse=True)
    return sorted(groups.values(), key=lambda g: g["total_value"], reverse=True)


def build_groups(
    board: list[dict],
    industry_fn=data_source.fetch_industry_map,
) -> list[dict]:
    """Dựng cấu trúc nhóm ngành từ board ĐÃ fetch sẵn (bản đồ ngành memoize).

    Tách khỏi _groups để market_refresher fetch board 1 lần rồi dựng cả 3 view
    (sectors/heatmap/sector_symbols) — không gọi price_board lại.
    """
    return _build_groups(board, get_industry_map(industry_fn))


def sectors_view(groups: list[dict]) -> list[dict]:
    """View danh sách nhóm ngành (nhẹ): tên, mã ICB, tổng value, số mã."""
    return [
        {
            "group": g["group"],
            "icb_code": g["icb_code"],
            "total_value": g["total_value"],
            "symbol_count": len(g["symbols"]),
        }
        for g in groups
    ]


def heatmap_view(groups: list[dict]) -> list[dict]:
    """View treemap bản đồ nhiệt: ngành → mã (symbol, change_pct, market_cap).

    market_cap tạm dùng 'value' (giá trị giao dịch lũy kế) làm đại lượng kích
    thước ô — đổi sang vốn hóa thật khi nguồn dữ liệu có số cổ phiếu lưu hành.
    """
    return [
        {
            "group": g["group"],
            "icb_code": g["icb_code"],
            "symbols": [
                {
                    "symbol": s["symbol"],
                    "change_pct": s.get("change_pct", 0),
                    "market_cap": s.get("value") or 0,
                }
                for s in g["symbols"]
            ],
        }
        for g in groups
    ]


def sector_symbols_view(groups: list[dict], icb_code: str) -> dict:
    """View các mã + OHLC phiên hôm nay của nhóm ICB cấp 3 trùng icb_code."""
    for g in groups:
        if g["icb_code"] == icb_code:
            return {"group": g["group"], "icb_code": icb_code, "data": g["symbols"]}
    return {"group": None, "icb_code": icb_code, "data": []}


def _groups(board_fn, industry_fn) -> list[dict]:
    """Lấy board VN100 + bản đồ ngành rồi dựng cấu trúc nhóm đầy đủ."""
    symbols = vn100_service.get_symbols()
    if not symbols:
        return []
    board = board_fn(symbols)
    if board is None:
        return []
    return _build_groups(board, get_industry_map(industry_fn))


def get_sectors(
    board_fn=data_source.fetch_vn100_board,
    industry_fn=data_source.fetch_industry_map,
) -> dict:
    """Danh sách nhóm ngành (nhẹ): tên, mã ICB, tổng value, số mã."""
    return sectors_view(_groups(board_fn, industry_fn))


def get_sector_symbols(
    icb_code: str,
    board_fn=data_source.fetch_vn100_board,
    industry_fn=data_source.fetch_industry_map,
) -> dict:
    """Các mã + OHLC phiên hôm nay của nhóm ICB cấp 3 trùng icb_code."""
    return sector_symbols_view(_groups(board_fn, industry_fn), icb_code)


def get_heatmap(
    board_fn=data_source.fetch_vn100_board,
    industry_fn=data_source.fetch_industry_map,
) -> list[dict]:
    """Cấu trúc cho treemap bản đồ nhiệt: ngành → mã (symbol, change_pct, market_cap)."""
    return heatmap_view(_groups(board_fn, industry_fn))
