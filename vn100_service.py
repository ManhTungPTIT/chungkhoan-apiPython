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


def get_board(fetch_fn=data_source.fetch_vn100_board) -> dict:
    """Lấy bảng VN100 trực tiếp: 1 request price_board → lọc + sort."""
    symbols = get_symbols()
    if not symbols:
        return {"data": []}
    board = fetch_fn(symbols)
    if board is None:
        return {"data": []}
    return {"data": _process(board)}
