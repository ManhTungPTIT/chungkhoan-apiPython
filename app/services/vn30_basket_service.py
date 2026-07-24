"""Chart "MÃ RỔ VN30" — 30 mã rổ VN30 xếp trên 4 panel DÙNG CHUNG trục Y (danh
sách mã), mỗi mã bốn con số:

  1. Giá trị khớp lệnh (Tỷ)   = value / 1e9   (value board = khớp lệnh, KHÔNG
                                               gộp thỏa thuận — thỏa thuận nằm ở
                                               snapshot `put_through` riêng)
  2. Đường giá hiện tại (Nghìn) = price / 1000
  3. % thay đổi = (price - ref) / ref * 100
  4. Tăng/Giảm giá = cờ màu theo `status` (panel trang trí, không mang số liệu)

Mã CHƯA KHỚP (price=0) → status "flat", % = 0. Nếu tính (0 - ref)/ref sẽ ra
-100% và mã hiện đỏ kịch, sai; giống market_status_service coi price=0 là đứng
giá.

Sắp theo % thay đổi GIẢM DẦN (mã tăng mạnh nhất trên cùng → giảm mạnh nhất dưới
cùng). Chỉ lấy mã thuộc rổ VN30; đọc market_board_full — KHÔNG request vendor.
"""

VND_TO_TY = 1e9
VND_TO_NGHIN = 1000


def _num(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def build_row(row) -> dict:
    price = _num(row.get("price"))
    ref = _num(row.get("ref"))
    value = _num(row.get("value"))

    if price > 0 and ref > 0:
        change_pct = round((price - ref) / ref * 100, 2)
    else:
        # Chưa khớp lệnh (price=0) hoặc thiếu tham chiếu → coi như đứng giá.
        change_pct = 0.0

    if change_pct > 0:
        status = "up"
    elif change_pct < 0:
        status = "down"
    else:
        status = "flat"

    return {
        "symbol": row.get("symbol"),
        "change_pct": change_pct,
        "value_ty": round(value / VND_TO_TY, 2),
        "price_nghin": round(price / VND_TO_NGHIN, 2),
        "status": status,
    }


def build_vn30_basket(board_rows, vn30_members) -> dict:
    """board_rows: market_board_full ({symbol, price, ref, value, ...}).
    vn30_members: danh sách 30 mã rổ VN30 (vn100_service.get_vn30_members()).

    Trả {rows: [...]} — mỗi mã đủ số liệu cho 4 panel, đã sắp theo % giảm dần.
    """
    members = set(vn30_members or [])
    rows = [
        build_row(row)
        for row in (board_rows or [])
        if row.get("symbol") in members
    ]
    rows.sort(key=lambda r: r["change_pct"], reverse=True)
    return {"rows": rows}


def get_vn30_basket() -> dict:
    """Điểm gọi runtime: đọc board TOÀN thị trường từ cache RAM
    (market_board_full) + danh sách VN30 (memoize) — không gọi vnstock thêm."""
    from app.data import market_cache
    from app.services import signal_service, vn100_service

    board_rows = market_cache.get_snapshot("market_board_full") or []
    result = build_vn30_basket(board_rows, vn100_service.get_vn30_members())
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
