"""Hai chart theo NGÀNH, cùng một phép gom nhóm:

1. "XU HƯỚNG DÒNG TIỀN — TÍCH CỰC TIÊU CỰC NGÀNH": 100% stacked ngang, mỗi hàng
   một ngành, chia theo 5 trạng thái giá. Đơn vị là **SỐ MÃ**, không phải tiền.
2. "TỔNG HỢP TĂNG GIẢM THEO NGÀNH": % thay đổi bình quân + giá trị khớp lệnh +
   giá bình quân của từng ngành.

Hai chart mâu thuẫn nhau là chuyện BÌNH THƯỜNG, không phải bug: một ngành có 80%
số mã tăng vẫn có thể âm tiền nếu mã lớn nhất giảm. Chart 1 đếm mã, chart 2 cân
theo tiền.

Phân loại 5 trạng thái DÙNG LẠI `bull_bear_service.classify_row` — không chép
lại logic, để định nghĩa "kịch trần/kịch sàn" không bao giờ lệch giữa các chart.

Ba con số của chart 2 (xem ghi chú thiết kế):
- change_pct = Σ(% thay đổi × GT khớp lệnh) / ΣGT — bình quân GIA QUYỀN theo tiền,
  phản ánh nơi tiền thực sự chảy. Bình quân đơn giản thì một mã penny thanh khoản
  gần 0 có trọng số ngang VCB.
- avg_price = ΣGT khớp lệnh / ΣKL khớp — giá bình quân gia quyền. Lấy mean(giá)
  thô thì một mã 200K kéo lệch cả ngành.
- value = ΣGT khớp lệnh.
"""

from app.services import bull_bear_service
from app.services import sector_service

UNCLASSIFIED = sector_service.UNCLASSIFIED

# Thứ tự stack CỐ ĐỊNH, theo ngữ nghĩa tích cực → tiêu cực, không sort theo giá
# trị. Đảo thứ tự này là các hàng lệch nhau, không so sánh được theo cột.
BREADTH_STATES = (
    {"key": "limit_up", "label": "Mã trần"},
    {"key": "up", "label": "Mã tăng"},
    {"key": "flat", "label": "Mã đứng giá"},
    {"key": "down", "label": "Mã giảm giá"},
    {"key": "limit_down", "label": "Mã sàn"},
)

_KEYS = tuple(s["key"] for s in BREADTH_STATES)

# bull_bear_service đặt tên theo "phe" (Bò Tím / Gấu Sàn…), chart này đặt tên
# theo trạng thái giá. Vẫn dùng CHUNG một hàm phân loại — gồm cả bước làm tròn
# về đơn vị đồng trước khi so bằng — rồi chỉ đổi tên khoá ở đây.
_STATE_OF_SIDE = {
    "bull_purple": "limit_up",
    "bull_green": "up",
    "neutral": "flat",
    "bear_red": "down",
    "bear_floor": "limit_down",
}


def classify_state(price, ref, ceiling, floor) -> str:
    return _STATE_OF_SIDE[bull_bear_service.classify_row(price, ref, ceiling, floor)]


def build_sector_breadth(board_rows, industry_map: dict | None = None) -> dict:
    """board_rows: market_board_full ({symbol, price, ref, ceiling, floor, value,
    volume}). Trả {industries: [...]} — mỗi ngành đủ số liệu cho CẢ HAI chart.

    Ngành xếp theo TỔNG GIÁ TRỊ KHỚP LỆNH giảm dần: đọc từ trên xuống là đi từ
    ngành hút tiền nhất tới ngành gần như không giao dịch, và hai chart luôn cùng
    một thứ tự hàng nên so ngang được. Đây là thứ tự DUY NHẤT cho cả hai chart —
    FE không sắp lại, muốn đổi thì đổi ở đây.

    Chốt hạ ties bằng số mã: các ngành value = 0 (chưa giao dịch) mới đụng nhau,
    xếp ngành nhiều mã lên trước cho kết quả ổn định giữa các lần refresh.
    """
    imap = industry_map or {}
    groups: dict[str, dict] = {}

    for row in board_rows or []:
        info = imap.get(row.get("symbol")) or {}
        icb_code = info.get("icb_code", "")
        key = icb_code or UNCLASSIFIED
        group = groups.get(key)
        if group is None:
            group = {
                "name": info.get("icb_name", UNCLASSIFIED),
                "icb_code": icb_code,
                "counts": {k: 0 for k in _KEYS},
                "count": 0,
                "value": 0.0,
                "volume": 0.0,
                "_change_weighted": 0.0,
            }
            groups[key] = group

        state = classify_state(
            row.get("price"), row.get("ref"), row.get("ceiling"), row.get("floor")
        )
        group["counts"][state] += 1
        group["count"] += 1

        value = row.get("value") or 0
        if value > 0:
            group["value"] += value
            group["volume"] += row.get("volume") or 0
            group["_change_weighted"] += (row.get("change_pct") or 0) * value

    industries = []
    for group in groups.values():
        count = group["count"]
        if count <= 0:
            continue
        value = group["value"]
        volume = group["volume"]
        industries.append(
            {
                "name": group["name"],
                "icb_code": group["icb_code"],
                "count": count,
                "counts": group["counts"],
                # % SỐ MÃ, làm tròn 2 chữ số. Nhóm rỗng vẫn giữ khoá (fillna(0))
                # — thiếu khoá là thứ tự stack lệch giữa các hàng.
                "pcts": {
                    k: round(group["counts"][k] / count * 100, 2) for k in _KEYS
                },
                "value": value,
                "volume": volume,
                "avg_price": (value / volume) if volume > 0 else 0.0,
                "change_pct": round(group["_change_weighted"] / value, 2) if value > 0 else 0.0,
            }
        )

    industries.sort(key=lambda g: (g["value"], g["count"]), reverse=True)
    return {"industries": industries}


def get_sector_breadth() -> dict:
    """Điểm gọi runtime: đọc board TOÀN thị trường từ cache RAM
    (market_board_full) — không gọi vnstock thêm."""
    from app.data import market_cache
    from app.services import signal_service

    board_rows = market_cache.get_snapshot("market_board_full") or []
    result = build_sector_breadth(board_rows, sector_service.get_industry_map())
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
