"""Phục vụ homepage — top mã VN100 theo khối lượng giao dịch + xu hướng.

Đọc thuần từ cache của signal_service (tính nền 1 lần/phiên: lịch sử VN100 đã
được fetch để tính tín hiệu, tiện thể lưu luôn khối lượng phiên gần nhất). Nhờ
vậy homepage không phải fetch lại ~100 mã → nhanh, né rate-limit.
"""

import data_source
import signal_service


def get_top_volume(limit: int = 10) -> dict:
    """Top `limit` mã theo khối lượng khớp phiên gần nhất, kèm xu hướng mua/bán.

    Mỗi phần tử: {symbol, volume, trend} với trend = "buy"/"sell"/None (mã chưa
    có tín hiệu). Sắp theo volume giảm dần.
    """
    volumes = signal_service.volumes_snapshot()
    rows = [
        {"symbol": sym, "volume": vol, "trend": signal_service.signal_of(sym)}
        for sym, vol in volumes.items()
    ]
    rows.sort(key=lambda r: r["volume"], reverse=True)
    return {"data": rows[:limit]}


def get_market_depth() -> dict:
    """Tổng cầu / tổng cung toàn thị trường = cộng dư mua/dư bán (3 bước giá mỗi
    bên) của mọi mã đang giao dịch. Dữ liệu sổ lệnh real-time (1 request, không
    cache). Fetch lỗi → trả 0."""
    symbols = data_source.fetch_all_symbols()
    board = data_source.fetch_market_bid_ask(symbols) if symbols else None
    if not board:
        return {"total_bid_volume": 0, "total_ask_volume": 0}
    return {
        "total_bid_volume": sum(r["bid_volume"] for r in board),
        "total_ask_volume": sum(r["ask_volume"] for r in board),
    }
