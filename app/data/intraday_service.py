"""Phục vụ nến lịch sử theo mã + khung thời gian — gọi vnstock trực tiếp, không cache.

Mỗi request gọi thẳng `data_source.fetch_intraday_history` với khung (interval) và
cửa sổ lookback tương ứng. Fetch hỏng (rate-limit/mạng → None) thì trả {data: []};
không bao giờ ném ra ngoài.
"""

from datetime import datetime, timedelta, timezone

from app.data import data_source

VN_TZ = timezone(timedelta(hours=7))
DEFAULT_INTERVAL = "1d"

VN_OFFSET_S = 7 * 60 * 60
DAY_S = 24 * 60 * 60

# Khung phải tự resample từ nến ngày ở dưới thay vì tin token vendor: probe
# 20/07/2026 xác nhận nguồn ASEAN (Quote(source="ASEAN").history) BỎ QUA
# resolution=W/M — trả về y hệt nến ngày dù gọi D/W/M (cùng timestamp, cách
# nhau 86400s). Vì vậy "vnstock" của 2 khung này luôn là "1D".
RESAMPLE_INTERVALS = {"1w", "1mth"}

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
    "1w": {"vnstock": "1D", "start": "2023-01-01"},
    "1mth": {"vnstock": "1D", "start": "2018-01-01"},
}


def _bucket_start(unix_time: int, interval: str) -> int:
    """Đầu khung tuần/tháng theo NGÀY GIAO DỊCH giờ VN — PHẢI khớp bucketStart
    ở frontend (chart/untils/liveCandle.js) để nến lịch sử (endpoint này) và
    nến ghép giá realtime (WS) dùng chung mốc thời gian, không lệch khung."""
    vn = unix_time + VN_OFFSET_S
    if interval == "1w":
        # Tuần bắt đầu thứ Hai; epoch (01/01/1970) là thứ Năm.
        days = vn // DAY_S
        return int((days - ((days + 3) % 7)) * DAY_S - VN_OFFSET_S)
    d = datetime.fromtimestamp(vn, tz=timezone.utc)
    month_start = datetime(d.year, d.month, 1, tzinfo=timezone.utc)
    return int(month_start.timestamp()) - VN_OFFSET_S


def _resample(rows: list[dict], interval: str) -> list[dict]:
    """Gộp nến ngày thành nến tuần/tháng: open=nến đầu khung, high=max,
    low=min, close=nến cuối khung, volume=tổng. Giữ nguyên thứ tự xuất hiện
    của khung đầu tiên gặp (rows đầu vào đã sắp theo thời gian tăng dần)."""
    if interval not in RESAMPLE_INTERVALS or not rows:
        return rows

    buckets: dict[int, dict] = {}
    order: list[int] = []
    for row in rows:
        time_value = row.get("time")
        if time_value is None:
            continue
        try:
            t = int(time_value)
            o, h, l, c = (float(row[k]) for k in ("open", "high", "low", "close"))
        except (TypeError, ValueError, KeyError):
            continue
        volume = float(row.get("volume") or 0)

        bucket = _bucket_start(t, interval)
        if bucket not in buckets:
            buckets[bucket] = {
                "time": bucket,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": volume,
            }
            order.append(bucket)
        else:
            b = buckets[bucket]
            b["high"] = max(b["high"], h)
            b["low"] = min(b["low"], l)
            b["close"] = c
            b["volume"] += volume

    return [buckets[bucket] for bucket in order]


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
    return {"data": _resample(data, interval)}
