"""Chart "DÒNG TIỀN PHE BÒ VÀ PHE GẤU" — cộng GIÁ TRỊ KHỚP LỆNH toàn thị trường
theo 5 phe, cộng thêm cột Tổng.

Khác market_status_service (đếm SỐ MÃ theo 5 trạng thái giá): ở đây cộng TIỀN, và
cách chia phe cũng khác — mã kịch trần/kịch sàn tách hẳn thành "Bò Tím"/"Gấu Sàn"
chứ không nằm trong "Bò Xanh"/"Gấu Đỏ".

Thứ tự if/elif là bản chất của phép phân loại, KHÔNG được đảo:

    giá >= trần        → Bò Tím
    giá >  tham chiếu  → Bò Xanh
    giá == tham chiếu  → Trung lập
    giá <= sàn         → Gấu Sàn
    còn lại            → Gấu Đỏ

Kiểm tra trần TRƯỚC khi so tăng, sàn TRƯỚC khi so giảm — nếu không, mã kịch trần
bị gộp nhầm vào Bò Xanh và cột Bò Tím luôn bằng 0.

Trần/sàn lấy thẳng từ bảng giá, không tự tính: biên độ khác nhau theo sàn (HOSE
±7%, HNX ±10%, UPCOM ±15%) và còn phải làm tròn theo bước giá.
"""

# Thứ tự cột CỐ ĐỊNH theo bản mẫu, không sort theo giá trị. Nhóm rỗng vẫn phải
# giữ cột (bản mẫu có "Phe Gấu Sàn" = 0).
BULL_BEAR_GROUPS = (
    {"key": "bull_green", "label": "Phe Bò Xanh"},
    {"key": "neutral", "label": "Phe Trung lập"},
    {"key": "bear_red", "label": "Phe Gấu Đỏ"},
    {"key": "bull_purple", "label": "Phe Bò Tím"},
    {"key": "bear_floor", "label": "Phe Gấu Sàn"},
)

_KEYS = tuple(g["key"] for g in BULL_BEAR_GROUPS)


def _round_price(value):
    """Làm tròn về đơn vị đồng trước khi so sánh bằng.

    Giá từ bảng giá đi qua phép chia thang đo nên có thể ra số thực (24749.999…);
    so sánh `==` trực tiếp sẽ trượt và mọi mã đứng giá rơi nhầm sang Gấu Đỏ.
    """
    try:
        return round(float(value or 0))
    except (TypeError, ValueError):
        return 0


def classify_row(price, ref, ceiling, floor) -> str:
    """Phe của một mã. price=0 (chưa khớp lệnh) coi như đứng giá tham chiếu."""
    price = _round_price(price)
    ref = _round_price(ref)
    ceiling = _round_price(ceiling)
    floor = _round_price(floor)

    if not price:
        return "neutral"
    if ceiling and price >= ceiling:
        return "bull_purple"
    if price > ref:
        return "bull_green"
    if price == ref:
        return "neutral"
    if floor and price <= floor:
        return "bear_floor"
    return "bear_red"


def compute_bull_bear(board_rows) -> dict:
    """board_rows: list of {price, ref, ceiling, floor, value} (market_board_full).

    Trả {total_value, groups: [{key, label, value, pct}]} — value đơn vị VND,
    pct là % trên tổng giá trị khớp lệnh toàn thị trường.
    """
    sums = {key: 0.0 for key in _KEYS}
    for row in board_rows or []:
        value = row.get("value") or 0
        if value <= 0:
            continue
        sums[classify_row(row.get("price"), row.get("ref"), row.get("ceiling"), row.get("floor"))] += value

    total = sum(sums.values())
    groups = [
        {
            "key": g["key"],
            "label": g["label"],
            "value": sums[g["key"]],
            "pct": round(sums[g["key"]] / total * 100, 2) if total else 0.0,
        }
        for g in BULL_BEAR_GROUPS
    ]
    return {"total_value": total, "groups": groups}


def get_bull_bear() -> dict:
    """Điểm gọi runtime: đọc board TOÀN thị trường từ cache RAM
    (market_board_full, dựng sẵn bởi market_refresher) — không gọi vnstock thêm."""
    from app.data import market_cache
    from app.services import signal_service

    board_rows = market_cache.get_snapshot("market_board_full") or []
    result = compute_bull_bear(board_rows)
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
