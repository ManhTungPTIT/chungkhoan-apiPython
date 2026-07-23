"""Chart "DÒNG TIỀN THEO NHÓM GIÁ CỔ PHIẾU" — cộng GIÁ TRỊ KHỚP LỆNH toàn thị
trường theo khoảng giá đóng cửa, cộng thêm cột Tổng.

Khoảng chia CỐ TÌNH không đều (10 → 20 → 40 → 60 → 80 → 100, bước 20K ở đoạn
giữa): mật độ mã tập trung ở vùng 10–40K nên chia đều sẽ dồn gần hết tiền vào
một hai cột.

Thứ tự nhóm là thứ tự khoảng giá tăng dần, KHÔNG sort theo giá trị.

Về mẫu số của %: cột Tổng ở đây bằng đúng tổng các nhóm, nên % luôn cộng đủ
100%. Mọi mã có giá trị khớp lệnh > 0 đều rơi vào đúng một khoảng (mã chưa khớp
có value = 0 nên bị loại từ đầu) — không có phần "lệch" giữa cột Tổng và tổng
các cột thành phần.
"""

# Ngưỡng trên (VND) của từng nhóm, tăng dần; nhóm cuối là phần còn lại.
# Nhãn cố tình ngắn ("≤20K" chứ không "Giá<=20K"): 8 cột trên một panel hẹp thì
# nhãn dài dính liền nhau ở trục hoành, đọc thành một chuỗi liền.
PRICE_BANDS = (
    {"key": "le_10k", "label": "≤10K", "max_price": 10_000},
    {"key": "le_20k", "label": "≤20K", "max_price": 20_000},
    {"key": "le_40k", "label": "≤40K", "max_price": 40_000},
    {"key": "le_60k", "label": "≤60K", "max_price": 60_000},
    {"key": "le_80k", "label": "≤80K", "max_price": 80_000},
    {"key": "le_100k", "label": "≤100K", "max_price": 100_000},
    {"key": "gt_100k", "label": ">100K", "max_price": None},
)

_KEYS = tuple(b["key"] for b in PRICE_BANDS)


def classify_price(price) -> str:
    """Nhóm giá của một mã. Biên là ĐÓNG bên phải: giá đúng 10.000 thuộc nhóm
    '≤10K', không rơi sang nhóm sau."""
    try:
        price = float(price or 0)
    except (TypeError, ValueError):
        price = 0.0
    for band in PRICE_BANDS:
        if band["max_price"] is not None and price <= band["max_price"]:
            return band["key"]
    return PRICE_BANDS[-1]["key"]


def compute_price_bands(board_rows) -> dict:
    """board_rows: list of {price, value} (market_board_full).

    Trả {total_value, groups: [{key, label, value, pct}]} — value đơn vị VND.
    """
    sums = {key: 0.0 for key in _KEYS}
    for row in board_rows or []:
        value = row.get("value") or 0
        if value <= 0:
            continue
        sums[classify_price(row.get("price"))] += value

    total = sum(sums.values())
    groups = [
        {
            "key": b["key"],
            "label": b["label"],
            "value": sums[b["key"]],
            "pct": round(sums[b["key"]] / total * 100, 2) if total else 0.0,
        }
        for b in PRICE_BANDS
    ]
    return {"total_value": total, "groups": groups}


def get_price_bands() -> dict:
    """Điểm gọi runtime: đọc board TOÀN thị trường từ cache RAM
    (market_board_full, dựng sẵn bởi market_refresher) — không gọi vnstock thêm."""
    from app.data import market_cache
    from app.services import signal_service

    board_rows = market_cache.get_snapshot("market_board_full") or []
    result = compute_price_bands(board_rows)
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
