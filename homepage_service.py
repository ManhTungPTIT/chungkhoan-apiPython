"""Phục vụ homepage — top mã VN100 theo khối lượng giao dịch + xu hướng.

Đọc thuần từ cache của signal_service (tính nền 1 lần/phiên: lịch sử VN100 đã
được fetch để tính tín hiệu, tiện thể lưu luôn khối lượng phiên gần nhất). Nhờ
vậy homepage không phải fetch lại ~100 mã → nhanh, né rate-limit.
"""

import data_source
import signal_service

# Index đại diện 3 sàn để cộng tổng KL toàn thị trường (mỗi index history có cột
# volume = tổng KL khớp của sàn đó).
_MARKET_INDICES = ("VNINDEX", "HNXINDEX", "UPCOMINDEX")


def get_top_volume(limit: int = 10) -> dict:
    """Top `limit` mã theo khối lượng khớp phiên gần nhất (cache), kèm xu hướng và
    giá trị sổ lệnh real-time.

    Mỗi phần tử: {symbol, volume, trend, buy_value, sell_value} với
      trend       — "buy"/"sell"/None (mã chưa có tín hiệu)
      buy_value   — tổng dư mua (3 mức) × giá mua cao nhất (bid_1)
      sell_value  — tổng dư bán (3 mức) × giá bán cao nhất (ask_3)
    buy_value/sell_value = None nếu không lấy được sổ lệnh. Sắp theo volume giảm dần.
    """
    volumes = signal_service.volumes_snapshot()
    rows = [
        {"symbol": sym, "volume": vol, "trend": signal_service.signal_of(sym)}
        for sym, vol in volumes.items()
    ]
    rows.sort(key=lambda r: r["volume"], reverse=True)
    top = rows[:limit]

    # Sổ lệnh real-time cho riêng top mã (1 request price_board).
    depth = data_source.fetch_market_bid_ask([r["symbol"] for r in top]) if top else None
    by_symbol = {d["symbol"]: d for d in depth} if depth else {}
    for row in top:
        d = by_symbol.get(row["symbol"])
        row["buy_value"] = d["bid_volume"] * d["max_bid_price"] if d else None
        row["sell_value"] = d["ask_volume"] * d["max_ask_price"] if d else None
    return {"data": top}


def get_market_depth() -> dict:
    """Tổng cầu / tổng cung toàn thị trường = cộng dư mua/dư bán (3 bước giá mỗi
    bên) của mọi mã đang giao dịch. Dữ liệu sổ lệnh real-time (1 request, không
    cache). Fetch lỗi → trả 0."""
    symbols = data_source.fetch_all_symbols()##lay dnah sach ma
    board = data_source.fetch_market_bid_ask(symbols) if symbols else None
    if not board:
        return {"total_bid_volume": 0, "total_ask_volume": 0}
    return {
        "total_bid_volume": sum(r["bid_volume"] for r in board),
        "total_ask_volume": sum(r["ask_volume"] for r in board),
    }


def get_market_breadth() -> dict:
    """Độ rộng thị trường (1 request price_board toàn thị trường):
      advancers/decliners/unchanged — số mã tăng/giảm/đứng giá so với hôm qua
        (giá khớp real-time vs giá tham chiếu).
      total_value          — tổng GIÁ TRỊ khớp lệnh phiên ĐANG diễn ra (real-time,
                             VND), cộng accumulated_value mọi mã.
      prev_total_volume    — tổng KHỐI LƯỢNG khớp lệnh phiên HÔM QUA (cộng volume
                             3 index).
    Fetch lỗi → 0."""
    symbols = data_source.fetch_all_symbols()
    board = data_source.fetch_vn100_board(symbols) if symbols else None
    advancers = decliners = unchanged = 0
    total_value = 0
    for row in board or []:
        total_value += row["value"]
        if not row["price"]:  # chưa khớp lệnh → coi như đứng giá tham chiếu
            unchanged += 1
        elif row["change_pct"] > 0:
            advancers += 1
        elif row["change_pct"] < 0:
            decliners += 1
        else:
            unchanged += 1
    return {
        "advancers": advancers,
        "decliners": decliners,
        "unchanged": unchanged,
        "total_value": total_value,
        "prev_total_volume": _prev_total_volume(),
    }


def _prev_total_volume() -> int:
    """Tổng KL khớp phiên hôm qua của cả thị trường = cộng volume 3 index. Bỏ qua
    index nào fetch lỗi (None)."""
    total = 0
    for index in _MARKET_INDICES:
        volume = data_source.fetch_prev_session_volume(index)
        if volume:
            total += volume
    return total
