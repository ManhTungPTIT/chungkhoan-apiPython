"""Chart "DIỄN BIẾN THỊ TRƯỜNG" — đếm số mã toàn thị trường (3 sàn gộp) theo 5
nhóm trạng thái giá, LOẠI TRỪ LẪN NHAU (mỗi mã rơi vào đúng 1 nhóm):

  limit_up    giá = trần
  up          giá > tham chiếu (và < trần)
  flat        giá = tham chiếu, hoặc mã chưa khớp lệnh (giá = 0)
  down        giá < tham chiếu (và > sàn)
  limit_down  giá = sàn

Phải kiểm tra trần/sàn TRƯỚC khi so sánh tăng/giảm — nếu không, mã kịch trần/sàn
sẽ bị đếm lẫn vào nhóm tăng/giảm và tổng 2 nhóm đó sẽ sai (không loại trừ lẫn
nhau như spec yêu cầu).
"""

_GROUPS = ("limit_up", "up", "flat", "down", "limit_down")


def classify_row(price, ref, ceiling, floor) -> str:
    if ceiling and price == ceiling:
        return "limit_up"
    if floor and price == floor:
        return "limit_down"
    if not price or price == ref:
        return "flat"
    return "up" if price > ref else "down"


def compute_market_status(board_rows) -> dict:
    """board_rows: list of {price, ref, ceiling, floor, ...} (dict con của
    market_board_full — có thừa field khác không sao, chỉ đọc 4 field trên)."""
    counts = {g: 0 for g in _GROUPS}
    for row in board_rows or []:
        group = classify_row(row["price"], row["ref"], row["ceiling"], row["floor"])
        counts[group] += 1

    total = len(board_rows or [])
    groups = {
        g: {"count": n, "pct": round(n / total * 100, 2) if total else 0.0}
        for g, n in counts.items()
    }
    return {"total": total, "groups": groups}


def get_market_status() -> dict:
    """Điểm gọi runtime: đọc board TOÀN thị trường từ cache RAM (market_board_full,
    dựng sẵn bởi market_refresher mỗi tick) — không gọi vnstock thêm."""
    from app.data import market_cache
    from app.services import signal_service

    board_rows = market_cache.get_snapshot("market_board_full") or []
    result = compute_market_status(board_rows)
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
