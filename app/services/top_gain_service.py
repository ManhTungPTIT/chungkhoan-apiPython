"""Chart "TOP TĂNG CAO NHẤT T+2" — top mã đang HOLD đúng T+2, tăng giá cao nhất.

Đọc CÙNG nguồn cache như các view khác (board_vn100 đã gắn signal + _history_candles
đã nạp RAM) → endpoint chỉ ĐỌC, không gọi vnstock theo request.

Mỗi mã (signal_hold=True VÀ signal_sessions==WINDOW) tính 3 số cho chart combo:
  - gia_tri_khop_lenh (Tỷ): tổng value khớp lệnh của WINDOW phiên gần nhất —
    phiên hôm nay (T+2) lấy `value` live từ board (VND); các phiên đã đóng lấy
    từ nến history (value ≈ volume × close × 1000, close đơn vị nghìn đồng).
  - gia_hien_tai (Nghìn): giá live board / PRICE_BOARD_SCALE (fallback nến cuối).
  - pct_tang (%): (giá hiện tại − close ngày báo T0) / close T0 × 100 — với mã
    T+2, ngày báo chính là "2 phiên trước" (khớp spec).

Xếp giảm dần theo pct_tang, cắt top N.
"""

from datetime import datetime

from app.services import signal_service

WINDOW = 2
DEFAULT_TOP_N = 30

# close nến ở đơn vị nghìn đồng; value = volume(cổ) × giá(VND/cổ) = volume × close × 1000.
_NGHIN_TO_VND = 1000
_VND_TO_TY = 1_000_000_000


def _t0_close(candles, signal_date):
    """close của nến NGÀY BÁO (T0). None nếu không tìm thấy / hỏng time."""
    for candle in candles:
        try:
            if signal_service._candle_date(candle["time"]) == signal_date:
                return candle.get("close")
        except (KeyError, TypeError, ValueError, OverflowError, OSError):
            continue
    return None


def _closed_session_value_vnd(candles, signal_date, today, keep):
    """Tổng value (VND) của các phiên ĐÃ ĐÓNG trong cửa sổ: nằm HẲN giữa ngày báo
    và hôm nay (T0 < ngày < today) — hôm nay lấy value live từ board (tránh đếm
    trùng). Giữ tối đa `keep` phiên gần nhất. Nến thiếu volume → góp 0 (cột value
    degrade mềm, tự đủ khi history có volume sau refresh)."""
    mids = []
    for candle in candles:
        try:
            day = signal_service._candle_date(candle["time"])
        except (KeyError, TypeError, ValueError, OverflowError, OSError):
            continue
        if signal_date < day < today:
            mids.append(candle)
    total = 0
    for candle in mids[-keep:] if keep > 0 else []:
        volume = candle.get("volume")
        close = candle.get("close")
        if volume and close:
            total += volume * close * _NGHIN_TO_VND
    return total


def compute_top_gain(board_rows, history_candles, now=None, top_n=DEFAULT_TOP_N, window=WINDOW):
    """Hàm thuần (dễ test): board_rows = rows view vn100 (đã gắn signal_*),
    history_candles = {mã: [nến...]} (nến có volume). Trả {rows, window}."""
    now = now or datetime.now(signal_service.VN_TZ)
    today = now.date().isoformat()
    scale = signal_service.PRICE_BOARD_SCALE

    rows = []
    for row in board_rows or []:
        if not (row.get("signal_hold") and row.get("signal_sessions") == window):
            continue
        symbol = row.get("symbol")
        candles = (history_candles or {}).get(symbol)
        if not candles:
            continue
        signal_date = str(row.get("signal_date") or "")[:10]
        if not signal_date:
            continue
        t0_close = _t0_close(candles, signal_date)
        if not t0_close or t0_close <= 0:
            continue

        price = row.get("price") or 0
        gia_hien_tai = price / scale if price > 0 else candles[-1].get("close", 0)
        if not gia_hien_tai or gia_hien_tai <= 0:
            continue
        pct = round((gia_hien_tai - t0_close) / t0_close * 100, 2)

        # value = phiên hôm nay (T+2, live từ board) + (window-1) phiên đã đóng.
        today_vnd = row.get("value") or 0
        closed_vnd = _closed_session_value_vnd(candles, signal_date, today, window - 1)
        gia_tri_ty = round((today_vnd + closed_vnd) / _VND_TO_TY, 3)

        rows.append(
            {
                "symbol": symbol,
                "gia_tri_khop_lenh": gia_tri_ty,
                "gia_hien_tai": round(gia_hien_tai, 2),
                "pct_tang": pct,
            }
        )

    rows.sort(key=lambda r: r["pct_tang"], reverse=True)
    return {"rows": rows[:top_n], "window": window}


def get_top_gain(top_n=DEFAULT_TOP_N, window=WINDOW):
    """Điểm gọi runtime: đọc board_vn100 (view vn100 đã gắn signal) + nến base RAM.
    Không gọi vnstock. Rỗng an toàn nếu cache chưa warm. window=2 (T+2)/3 (T+3)."""
    from app.data import market_cache

    # Ưu tiên view vn100 trong market_wide (tươi ~20s, signal cắt trong phiên) →
    # fallback board_vn100 (~1 tiếng) — cùng thứ tự ưu tiên như endpoint /vn100.
    wide = market_cache.get_snapshot("market_wide") or {}
    snap = market_cache.get_snapshot("board_vn100") or {}
    vn100_view = wide.get("vn100") or snap.get("vn100") or {}
    board_rows = vn100_view.get("data") or []
    history = getattr(signal_service, "_history_candles", None) or {}
    result = compute_top_gain(board_rows, history, top_n=top_n, window=window)
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
