"""Chart "DÒNG TIỀN GIAO DỊCH THỎA THUẬN" — treemap theo MÃ, ô to theo giá trị
thỏa thuận trong phiên.

Khác hẳn /sectors (giá trị KHỚP LỆNH gom theo ngành): ở đây chỉ tính giao dịch
thỏa thuận (put-through), lấy từ bảng thỏa thuận riêng của sở, gom theo mã.

Hai bộ lọc bắt buộc, đừng bỏ:
  - Chỉ giữ mã cổ phiếu 3 ký tự chữ. Bảng thỏa thuận HNX/UPCOM phần lớn là TRÁI
    PHIẾU doanh nghiệp (LPB125006, VCK125005, BAB124016…) và chúng thường to
    hơn cổ phiếu — không lọc thì biểu đồ toàn trái phiếu.
  - Ngưỡng giá trị tối thiểu. Bảng thỏa thuận có cả lệnh lô lẻ vài nghìn đồng
    (SCR 4.380đ), vẽ ra thì ô nhỏ đến mức không đọc được.
"""

from app.data import data_source
from app.services import sector_service

# Ngưỡng mặc định: 20 tỷ đồng (theo spec biểu đồ). Đổi được qua tham số API.
DEFAULT_MIN_VALUE = 20_000_000_000

UNCLASSIFIED = sector_service.UNCLASSIFIED


def is_stock_symbol(symbol) -> bool:
    """Mã cổ phiếu VN = đúng 3 ký tự chữ. Loại trái phiếu (LPB125006), chứng
    quyền (CVNM2314), phái sinh (VN30F1M) khỏi bảng thỏa thuận."""
    return isinstance(symbol, str) and len(symbol) == 3 and symbol.isalpha()


def build_put_through(
    deals: list[dict],
    industry_map: dict | None = None,
    min_value: int = DEFAULT_MIN_VALUE,
) -> list[dict]:
    """Gom từng lệnh thỏa thuận theo mã, lọc ngưỡng, xếp giảm dần theo giá trị.

    deals: [{symbol, exchange, value, volume}] — output của fetch_put_through.
    Trả [{symbol, exchange, value, volume, deal_count, group, icb_code}].
    """
    imap = industry_map or {}
    totals: dict[str, dict] = {}
    for deal in deals or []:
        symbol = deal.get("symbol") if isinstance(deal, dict) else None
        if not is_stock_symbol(symbol):
            continue
        value = deal.get("value") or 0
        if value <= 0:
            continue
        item = totals.get(symbol)
        if item is None:
            info = imap.get(symbol) or {}
            item = {
                "symbol": symbol,
                "exchange": deal.get("exchange") or "",
                "value": 0,
                "volume": 0,
                "deal_count": 0,
                "group": info.get("icb_name", UNCLASSIFIED),
                "icb_code": info.get("icb_code", ""),
            }
            totals[symbol] = item
        item["value"] += value
        item["volume"] += deal.get("volume") or 0
        item["deal_count"] += 1

    kept = [item for item in totals.values() if item["value"] >= min_value]
    return sorted(kept, key=lambda x: x["value"], reverse=True)


def get_put_through(
    min_value: int = DEFAULT_MIN_VALUE,
    fetch_fn=data_source.fetch_put_through,
    industry_fn=data_source.fetch_industry_map,
) -> list[dict]:
    """Fetch trực tiếp (đường fallback khi snapshot cache còn trống)."""
    deals = fetch_fn()
    if deals is None:
        return []
    return build_put_through(deals, sector_service.get_industry_map(industry_fn), min_value)
