"""Phục vụ bảng VN100 — gọi vnstock trực tiếp, không cache.

Danh sách mã VN100 thay đổi rất hiếm (~mỗi quý) nên được memoize ở biến
module-level: lấy 1 lần rồi giữ trong RAM. Mỗi lần lấy bảng giá là 1 request
`price_board` cho toàn bộ mã. Fetch hỏng (rate-limit/mạng) → trả {data: []}.
"""

import data_source

VALUE_THRESHOLD = 5_000_000_000

_symbols: list[str] = []


def _process(board: list[dict]) -> list[dict]:
    """Lọc giá trị giao dịch > 5 tỷ, sắp theo value giảm dần."""
    filtered = [x for x in board if (x.get("value") or 0) > VALUE_THRESHOLD]
    return sorted(filtered, key=lambda x: x["value"] or 0, reverse=True)


def get_symbols(fetch_fn=data_source.fetch_vn100_symbols) -> list[str]:
    """Lấy danh sách mã VN100 1 lần rồi memoize.

    Fetch hỏng (trả None) → giữ rỗng để lần gọi sau thử lại (self-heal).
    """
    global _symbols
    if not _symbols:
        fetched = fetch_fn()
        if fetched:
            _symbols = fetched
    return _symbols


def get_active_symbols(fetch_fn=data_source.fetch_vn100_board) -> list[str]:
    """Mã VN100 có value giao dịch > VALUE_THRESHOLD — cùng tập với bảng hiển thị.

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
    """Dựng payload /vn100 từ board ĐÃ fetch sẵn: lọc value + sort + gắn tín hiệu.

    Tách khỏi get_board để market_refresher fetch board 1 lần rồi tái dùng cho
    nhiều view (vn100/sectors/heatmap) — không gọi price_board lại.

    Tín hiệu mua/bán (field `signal`) lấy từ cache signal_service (tính nền 1 lần/
    phiên) — import trễ để tránh vòng lặp import (signal_service cần vn100_service).
    """
    import signal_service

    return {"data": signal_service.attach_signals(_process(board))}


def get_board(fetch_fn=data_source.fetch_vn100_board) -> dict:
    """Lấy bảng VN100 trực tiếp: 1 request price_board → lọc + sort + gắn tín hiệu."""
    symbols = get_symbols()
    if not symbols:
        return {"data": []}
    board = fetch_fn(symbols)
    if board is None:
        return {"data": []}
    return build_board(board)
