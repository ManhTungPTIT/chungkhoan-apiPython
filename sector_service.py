"""Phục vụ nhóm ngành hay giao dịch — gom rổ VN100 theo ICB cấp 3.

Hai cách dùng, cùng một nguồn dữ liệu (1 request price_board + bản đồ ngành):
- get_sectors(): danh sách nhóm ngành xếp theo tổng giá trị giao dịch giảm dần.
- get_sector_symbols(icb_code): các mã + OHLC phiên hôm nay của một nhóm.

Bản đồ ngành (mã → ICB cấp 3) đổi rất hiếm nên được memoize ở biến module-level,
giống danh sách VN100 trong vn100_service. Fetch hỏng (None) → trả {data: []}.
"""

import data_source
import vn100_service

UNCLASSIFIED = "Chưa phân loại"

_industry_map: dict = {}


def get_industry_map(fetch_fn=data_source.fetch_industry_map) -> dict:
    """Lấy bản đồ mã → nhóm ICB cấp 3 1 lần rồi memoize.

    Fetch hỏng (None) → giữ rỗng để lần gọi sau thử lại (self-heal).
    """
    global _industry_map
    if not _industry_map:
        fetched = fetch_fn()
        if fetched:
            _industry_map = fetched
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
    data = [
        {
            "group": g["group"],
            "icb_code": g["icb_code"],
            "total_value": g["total_value"],
            "symbol_count": len(g["symbols"]),
        }
        for g in _groups(board_fn, industry_fn)
    ]
    return  data


def get_sector_symbols(
    icb_code: str,
    board_fn=data_source.fetch_vn100_board,
    industry_fn=data_source.fetch_industry_map,
) -> dict:
    """Các mã + OHLC phiên hôm nay của nhóm ICB cấp 3 trùng icb_code."""
    for g in _groups(board_fn, industry_fn):
        if g["icb_code"] == icb_code:
            return {"group": g["group"], "icb_code": icb_code, "data": g["symbols"]}
    return {"group": None, "icb_code": icb_code, "data": []}


def get_heatmap(
    board_fn=data_source.fetch_vn100_board,
    industry_fn=data_source.fetch_industry_map,
) -> list[dict]:
    """Cấu trúc cho treemap bản đồ nhiệt: ngành → mã (symbol, change_pct, market_cap).

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
        for g in _groups(board_fn, industry_fn)
    ]
