"""Phục vụ bảng giá rổ VNALL + toàn sàn HNX (~593 mã) — gọi vnstock trực tiếp,
không cache.

Tên module/endpoint giữ `vn100` cho tương thích (trước đây rổ VN100); nội dung
đã mở rộng lên VNALL + HNX. Danh sách mã thay đổi rất hiếm (~mỗi quý) nên được
memoize ở biến module-level: lấy 1 lần rồi giữ trong RAM. Mỗi lần lấy bảng giá
là 1 request `price_board` cho toàn bộ mã. Fetch hỏng (rate-limit/mạng) → trả
{data: []}.
"""

import data_source

VALUE_THRESHOLD = 1_000_000_000

_symbols: list[str] = []

_vn100_members: list[str] = []


def _process(board: list[dict]) -> list[dict]:
    """Lọc giá trị giao dịch > 1 tỷ, sắp theo value giảm dần."""
    filtered = [x for x in board if (x.get("value") or 0) > VALUE_THRESHOLD]
    return sorted(filtered, key=lambda x: x["value"] or 0, reverse=True)


def get_symbols(fetch_fn=data_source.fetch_vn100_symbols) -> list[str]:
    """Lấy danh sách mã rổ VNALL + HNX (~593 mã) 1 lần rồi memoize.

    Fetch hỏng (trả None) → giữ rỗng để lần gọi sau thử lại (self-heal).
    """
    global _symbols
    if not _symbols:
        fetched = fetch_fn()
        if fetched:
            _symbols = fetched
    return _symbols


def get_vn100_members(fetch_fn=data_source.fetch_vn100_members) -> list[str]:
    """Danh sách 100 mã rổ VN100 — memoize 1 lần, phục vụ cờ `vn100` trên board
    (bản đồ sức mạnh dòng tiền chỉ hiển thị mã VN100).

    Fetch hỏng (trả None) → giữ rỗng để lần gọi sau thử lại (self-heal);
    khi rỗng, build_board gắn vn100=False toàn bộ và FE degrade mềm.
    """
    global _vn100_members
    if not _vn100_members:
        fetched = fetch_fn()
        if fetched:
            _vn100_members = fetched
    return _vn100_members


def get_active_symbols(fetch_fn=data_source.fetch_vn100_board) -> list[str]:
    """Mã trong rổ có value giao dịch > VALUE_THRESHOLD — cùng tập với bảng hiển thị.

    1 request price_board để lấy value rồi lọc + sort. Dùng cho signal_service:
    chỉ tính tín hiệu cho các mã đang giao dịch đủ lớn (né mã thanh khoản thấp).
    Không có mã nền / board fetch lỗi (None) → [] để lần sau thử lại (self-heal).
    """
    symbols = get_symbols()
    if not symbols:
        return []
    board = fetch_fn(symbols)
    if not board:
        return []
    return [x["symbol"] for x in _process(board)]


def build_board(board: list[dict]) -> dict:
    """Dựng payload /vn100 từ board ĐÃ fetch sẵn: lọc value + sort + gắn cờ VN100
    + gắn tín hiệu.

    Tách khỏi get_board để market_refresher fetch board 1 lần rồi tái dùng cho
    nhiều view (vn100/sectors/heatmap) — không gọi price_board lại.

    Cờ `vn100` (bool) đánh dấu mã thuộc rổ VN100 — trang bản đồ sức mạnh dòng
    tiền lọc theo cờ này; chưa lấy được danh sách (rỗng) → False toàn bộ.

    Tín hiệu mua/bán (field `signal`) lấy từ cache signal_service (tính nền 1 lần/
    phiên) — import trễ để tránh vòng lặp import (signal_service cần vn100_service).
    """
    import signal_service

    members = set(get_vn100_members())
    rows = _process(board)
    for row in rows:
        row["vn100"] = row["symbol"] in members
    return {"data": signal_service.attach_signals(rows)}


def build_power_board(board: list[dict]) -> dict:
    """Dựng payload /power (bản đồ sức mạnh dòng tiền) từ board ĐÃ fetch sẵn:
    lọc value + sort (cùng _process với /vn100) rồi giữ lại mã thuộc rổ VN100,
    mỗi dòng chỉ 4 field symbol/price/change_pct/value — payload nhỏ, không
    kèm signal (trang power không dùng).

    Chưa lấy được danh sách VN100 (rỗng) → giữ nguyên board đã lọc value
    (degrade mềm, tự lành khi get_vn100_members lấy được ở chu kỳ sau).
    """
    members = set(get_vn100_members())
    rows = _process(board)
    if members:
        rows = [r for r in rows if r["symbol"] in members]
    return {
        "data": [
            {
                "symbol": r["symbol"],
                "price": r.get("price"),
                "change_pct": r.get("change_pct"),
                "value": r.get("value"),
            }
            for r in rows
        ]
    }


def get_power_board(fetch_fn=data_source.fetch_vn100_board) -> dict:
    """Lấy payload /power trực tiếp (fallback khi cache market_wide trống):
    1 request price_board cho đúng rổ VN100 (~100 mã) → build_power_board.

    Members chưa lấy được / fetch lỗi → {data: []} (FE giữ data cũ qua
    react-query, lần poll sau tự lành)."""
    members = get_vn100_members()
    if not members:
        return {"data": []}
    board = fetch_fn(members)
    if board is None:
        return {"data": []}
    return build_power_board(board)


def get_board(fetch_fn=data_source.fetch_vn100_board) -> dict:
    """Lấy bảng VN100 trực tiếp: 1 request price_board → lọc + sort + gắn tín hiệu."""
    symbols = get_symbols()
    if not symbols:
        return {"data": []}
    board = fetch_fn(symbols)
    if board is None:
        return {"data": []}
    return build_board(board)
