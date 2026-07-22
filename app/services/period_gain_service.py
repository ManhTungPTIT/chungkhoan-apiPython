"""Chart "TOP TĂNG CAO NHẤT TUẦN" (và sau này THÁNG) — toàn board sắp theo % tăng
trong kỳ.

Khác top_gain_service (lọc HOLD T+N): ở đây KHÔNG lọc tín hiệu, lấy cả board có
lịch sử, so giá cuối kỳ với giá ĐẦU kỳ (phiên đầu tiên của tuần/tháng).

Đọc cùng cache RAM (market_wide/board_vn100 + _history_candles), không gọi vnstock
theo request. Nến history là nến NGÀY có volume (xem signal_service._to_candles).

Mỗi mã:
  - gia_tri_khop_lenh (Tỷ): SUM value phiên trong kỳ — phiên đã đóng lấy từ history
    (volume × close × 1000, close nghìn đồng → VND), phiên hôm nay lấy value live
    từ board (VND, tránh đếm trùng).
  - gia_hien_tai (Nghìn): giá live board / PRICE_BOARD_SCALE (fallback nến cuối).
  - pct_tang (%): (giá hiện tại − close phiên đầu kỳ) / close đầu kỳ × 100.
"""

from datetime import date, datetime, timedelta

from app.services import signal_service

DEFAULT_TOP_N = 30

_NGHIN_TO_VND = 1000
_VND_TO_TY = 1_000_000_000


def period_start(period, now=None):
    """Ngày đầu kỳ (giờ VN): 'week' → thứ Hai tuần này; 'month' → mùng 1 tháng này."""
    now = now or datetime.now(signal_service.VN_TZ)
    today = now.date()
    if period == "month":
        return today.replace(day=1)
    # mặc định week
    return today - timedelta(days=today.weekday())


def _period_candles(candles, start_iso, today_iso):
    """Nến ĐÃ ĐÓNG trong kỳ: start_iso <= ngày < today (hôm nay lấy live từ board).
    Trả list theo thứ tự thời gian (candles vốn tăng dần)."""
    out = []
    for candle in candles:
        try:
            day = signal_service._candle_date(candle["time"])
        except (KeyError, TypeError, ValueError, OverflowError, OSError):
            continue
        if start_iso <= day < today_iso:
            out.append(candle)
    return out


def compute_period_gain(board_rows, history_candles, start, now=None, top_n=DEFAULT_TOP_N):
    """Hàm thuần: board_rows = rows board (symbol/price/value), history_candles =
    {mã: [nến ngày có volume]}, start = ngày đầu kỳ (date). Trả {rows, start}."""
    now = now or datetime.now(signal_service.VN_TZ)
    today_iso = now.date().isoformat()
    start_iso = start.isoformat() if isinstance(start, date) else str(start)[:10]
    scale = signal_service.PRICE_BOARD_SCALE

    rows = []
    for row in board_rows or []:
        symbol = row.get("symbol")
        candles = (history_candles or {}).get(symbol)
        if not candles:
            continue
        period = _period_candles(candles, start_iso, today_iso)
        if not period:
            continue
        start_close = period[0].get("close")
        if not start_close or start_close <= 0:
            continue

        price = row.get("price") or 0
        gia_hien_tai = price / scale if price > 0 else candles[-1].get("close", 0)
        if not gia_hien_tai or gia_hien_tai <= 0:
            continue
        pct = round((gia_hien_tai - start_close) / start_close * 100, 2)

        closed_vnd = 0
        for candle in period:
            volume = candle.get("volume")
            close = candle.get("close")
            if volume and close:
                closed_vnd += volume * close * _NGHIN_TO_VND
        today_vnd = row.get("value") or 0
        gia_tri_ty = round((closed_vnd + today_vnd) / _VND_TO_TY, 3)

        rows.append(
            {
                "symbol": symbol,
                "gia_tri_khop_lenh": gia_tri_ty,
                "gia_hien_tai": round(gia_hien_tai, 2),
                "pct_tang": pct,
            }
        )

    rows.sort(key=lambda r: r["pct_tang"], reverse=True)
    return {"rows": rows[:top_n], "start": start_iso}


def get_period_gain(period="week", top_n=DEFAULT_TOP_N):
    """Điểm gọi runtime: đọc board (market_wide→board_vn100) + nến base RAM."""
    from app.data import market_cache

    wide = market_cache.get_snapshot("market_wide") or {}
    snap = market_cache.get_snapshot("board_vn100") or {}
    vn100_view = wide.get("vn100") or snap.get("vn100") or {}
    board_rows = vn100_view.get("data") or []
    history = getattr(signal_service, "_history_candles", None) or {}

    start = period_start(period)
    result = compute_period_gain(board_rows, history, start, top_n=top_n)
    result["period"] = period
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
