"""Phục vụ nến lịch sử theo mã + khung thời gian — gọi vnstock trực tiếp, không cache.

Mỗi request gọi thẳng `data_source.fetch_intraday_history` với khung (interval) và
cửa sổ lookback tương ứng. Fetch hỏng (rate-limit/mạng → None) thì trả {data: []};
không bao giờ ném ra ngoài.
"""

from datetime import datetime, timedelta, timezone

import data_source

VN_TZ = timezone(timedelta(hours=7))
DEFAULT_INTERVAL = "1d"

# token (FE) → vnstock interval + cửa sổ dữ liệu.
# "lookback_days": start = hôm nay − N ngày; "start": ngày cố định.
# Chỉ 8 khung vnstock (VCI) hỗ trợ sẵn; token lạ → fallback DEFAULT_INTERVAL.
INTERVALS = {
    "1m": {"vnstock": "1m", "lookback_days": 5},
    "5m": {"vnstock": "5m", "lookback_days": 5},
    "15m": {"vnstock": "15m", "lookback_days": 30},
    "30m": {"vnstock": "30m", "lookback_days": 30},
    "1h": {"vnstock": "1H", "lookback_days": 90},
    "1d": {"vnstock": "1D", "start": "2025-01-01"},
    "1w": {"vnstock": "1W", "start": "2023-01-01"},
    "1mth": {"vnstock": "1M", "start": "2018-01-01"},
}


def get_intraday(symbol: str, interval: str = DEFAULT_INTERVAL, fetch_fn=None, now=None) -> dict:
    # Giải mặc định lúc gọi để test có thể inject fetch_fn/now giả.
    fetch_fn = fetch_fn or data_source.fetch_intraday_history
    now = now or datetime.now(VN_TZ)

    cfg = INTERVALS.get(interval) or INTERVALS[DEFAULT_INTERVAL]
    end = now.strftime("%Y-%m-%d")
    if "start" in cfg:
        start = cfg["start"]
    else:
        start = (now - timedelta(days=cfg["lookback_days"])).strftime("%Y-%m-%d")

    data = fetch_fn(symbol, start, end, interval=cfg["vnstock"])
    if data is None:
        return {"data": []}
    return {"data": data}
