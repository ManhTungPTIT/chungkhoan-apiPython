"""Phục vụ homepage — top mã VN100 theo khối lượng giao dịch + xu hướng.

Đọc thuần từ cache của signal_service (tính nền 1 lần/phiên: lịch sử VN100 đã
được fetch để tính tín hiệu, tiện thể lưu luôn khối lượng phiên gần nhất). Nhờ
vậy homepage không phải fetch lại ~100 mã → nhanh, né rate-limit.
"""

from app.data import data_source
from app.services import signal_service

# Index đại diện 3 sàn để cộng tổng KL toàn thị trường (mỗi index history có cột
# volume = tổng KL khớp của sàn đó).
_MARKET_INDICES = ("VNINDEX", "HNXINDEX", "UPCOMINDEX")

##La ban dong tien
def build_top_volume(volumes: dict, bid_ask: list[dict] | None, limit: int = 10) -> dict:
    """Dựng top-volume từ dữ liệu ĐÃ có sẵn: {symbol: volume} + sổ lệnh (bid/ask).

    Tách khỏi get_top_volume để market_refresher tái dùng bid/ask của market_wide
    snapshot (đã fetch chung) — không gọi price_board lại. `bid_ask` có thể chứa
    toàn thị trường hoặc chỉ top mã; tra theo symbol nên cả hai đều đúng.
    """
    rows = [
        {"symbol": sym, "volume": vol, "trend": signal_service.signal_of(sym)}
        for sym, vol in volumes.items()
    ]
    rows.sort(key=lambda r: r["volume"], reverse=True)
    top = rows[:limit]

    by_symbol = {d["symbol"]: d for d in (bid_ask or [])}
    for row in top:
        d = by_symbol.get(row["symbol"])
        row["buy_value"] = d["bid_volume"] * d["max_bid_price"] if d else None
        row["sell_value"] = d["ask_volume"] * d["max_ask_price"] if d else None
    return {"data": top}


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
    # Chỉ cần sổ lệnh cho top mã → fetch riêng nhóm này (1 request price_board).
    top_syms = [
        sym for sym, _ in sorted(volumes.items(), key=lambda kv: kv[1], reverse=True)[:limit]
    ]
    bid_ask = data_source.fetch_market_bid_ask(top_syms) if top_syms else None
    return build_top_volume(volumes, bid_ask, limit)


##can bang dong tien
def build_market_depth(bid_ask: list[dict] | None) -> dict:
    """Tổng cầu / tổng cung = cộng dư mua/dư bán (3 bước giá) từ sổ lệnh ĐÃ fetch."""
    if not bid_ask:
        return {"total_bid_volume": 0, "total_ask_volume": 0}
    return {
        "total_bid_volume": sum(r["bid_volume"] for r in bid_ask),
        "total_ask_volume": sum(r["ask_volume"] for r in bid_ask),
    }


def get_market_depth() -> dict:
    """Tổng cầu / tổng cung toàn thị trường = cộng dư mua/dư bán (3 bước giá mỗi
    bên) của mọi mã đang giao dịch. Dữ liệu sổ lệnh real-time. Fetch lỗi → trả 0."""
    symbols = data_source.fetch_all_symbols()##lay dnah sach ma
    bid_ask = data_source.fetch_market_bid_ask(symbols) if symbols else None
    return build_market_depth(bid_ask)


def build_market_breadth(board: list[dict] | None, prev_total_vol: int) -> dict:
    """Độ rộng thị trường từ board ĐÃ fetch sẵn + KL hôm qua tính sẵn.

      advancers/decliners/unchanged — số mã tăng/giảm/đứng giá so với tham chiếu.
      total_value       — tổng GIÁ TRỊ khớp lệnh phiên ĐANG diễn ra (real-time, VND).
      prev_total_volume — tổng KHỐI LƯỢNG khớp phiên HÔM QUA (cộng volume 3 index).
    """
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
        "prev_total_volume": prev_total_vol,
    }


def get_market_breadth() -> dict:
    """Độ rộng thị trường (1 request price_board toàn thị trường). Fetch lỗi → 0."""
    symbols = data_source.fetch_all_symbols()
    board = data_source.fetch_vn100_board(symbols) if symbols else None
    return build_market_breadth(board, prev_total_volume())


def prev_total_volume() -> int:
    """Tổng KL khớp phiên hôm qua của cả thị trường = cộng volume 3 index. Bỏ qua
    index nào fetch lỗi (None)."""
    total = 0
    for index in _MARKET_INDICES:
        volume = data_source.fetch_prev_session_volume(index)
        if volume:
            total += volume
    return total
