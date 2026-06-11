"""Phục vụ nến lịch sử theo mã — gọi vnstock trực tiếp, không cache.

Mỗi request gọi thẳng `data_source.fetch_intraday_history`. Fetch hỏng
(rate-limit/mạng → None) thì trả {data: []}; không bao giờ ném ra ngoài.
"""

from datetime import datetime, timedelta, timezone

import data_source

HISTORY_START = "2025-01-01"
VN_TZ = timezone(timedelta(hours=7))


def get_intraday(symbol: str, fetch_fn=None) -> dict:
    # Giải mặc định lúc gọi để test có thể inject fetch_fn giả.
    fetch_fn = fetch_fn or data_source.fetch_intraday_history
    end = datetime.now(VN_TZ).strftime("%Y-%m-%d")
    data = fetch_fn(symbol, HISTORY_START, end)
    if data is None:
        return {"data": []}
    return {"data": data}
