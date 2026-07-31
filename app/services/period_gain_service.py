"""Chart "TOP TĂNG MẠNH NHẤT TUẦN" (và biến thể THÁNG) — toàn rổ xếp theo ĐIỂM
(tăng giá có trọng số thanh khoản), CÙNG công thức xếp hạng với top_gain_service.

Khác top_gain_service ở MỐC so sánh: ở đây mốc là close phiên ĐẦU của một DẢI
phiên (week: phiên cũ nhất trong 5 phiên đã đóng — đúng nghĩa "close T-5"), còn
top_gain_service lùi hẳn (N+1) phiên.

Kỳ `week` (2026-07-27 đổi): **5 phiên ĐÃ ĐÓNG gần nhất, KHÔNG tính phiên hôm
nay**. Trước đây là "từ thứ Hai tuần này" — cách đó làm chart TRỐNG nguyên ngày
thứ Hai (đầu kỳ trùng hôm nay mà hôm nay lại bị loại) và cho kỳ dài ngắn thất
thường theo thứ trong tuần.

Kỳ `month` giữ nguyên cách tính theo lịch (từ mùng 1 tới phiên đã đóng gần nhất).
Endpoint vẫn nhận nhưng hiện chưa có chart nào dùng — nó ăn theo cùng công thức
điểm, chỉ khác cửa sổ.

Đọc cùng cache RAM (rổ view vn100 + `_history_candles`), không gọi vnstock theo
request. Nến history là nến NGÀY có volume (xem `signal_service._to_candles`).

Hai bộ lọc của chart (2026-07-31, cùng bộ với top_gain_service):
  - thanh khoản HÔM NAY > 1 tỷ — chặn phần carry-over chưa khớp đủ 1 tỷ hôm nay;
  - giá tăng (pct_tang > 0). Cùng nhau chúng đảm bảo diem luôn dương, không có
    nghịch lý "giảm sâu + thanh khoản lớn = điểm cao".

Mỗi mã:
  - gia_tri_khop_lenh (Tỷ): value khớp lệnh HÔM NAY (tại thời điểm trong phiên),
    lấy live từ board. 2026-07-31 đổi: trước đây là SUM value 5 phiên trong cửa
    sổ lấy từ history. Điểm nhân với thanh khoản HIỆN TẠI, nên cột hiển thị phải
    là chính con số đó — giữ số cộng dồn thì chart không giải thích nổi thứ hạng
    của chính nó (bar ngắn lại xếp trên bar dài).
  - gia_hien_tai (Nghìn): giá live board / PRICE_BOARD_SCALE (fallback close
    phiên cuối cửa sổ).
  - pct_tang (%): (giá hiện tại − close phiên ĐẦU cửa sổ) / close đó × 100.
  - diem: pct_tang × log10(thanh khoản Tỷ + 1) — số dùng để XẾP HẠNG.

Hệ số log10 tính theo TỶ đồng chứ không phải VND; lý do ghi trong docstring
top_gain_service (theo VND mọi mã rơi vào 9…12, xếp hạng thoái hoá về đúng thứ
tự % tăng).
"""

import math
from datetime import date, datetime, timedelta

from app.services import signal_service, top_gain_service

DEFAULT_TOP_N = 30
# Số phiên đã đóng của kỳ "week" — 1 tuần giao dịch.
RECENT_SESSIONS = 5

_VND_TO_TY = 1_000_000_000


def period_start(period, now=None):
    """Ngày đầu kỳ theo LỊCH (giờ VN) — chỉ còn dùng cho 'month': mùng 1 tháng
    này. Giữ 'week' → thứ Hai tuần này cho tương thích test/caller cũ; luồng tính
    của kỳ week KHÔNG dùng hàm này nữa (xem RECENT_SESSIONS)."""
    now = now or datetime.now(signal_service.VN_TZ)
    today = now.date()
    if period == "month":
        return today.replace(day=1)
    return today - timedelta(days=today.weekday())


def _period_candles(candles, start_iso, today_iso):
    """Nến ĐÃ ĐÓNG trong kỳ lịch: start_iso <= ngày < today. Trả theo thứ tự thời
    gian (candles vốn tăng dần). Dùng cho kỳ 'month'."""
    out = []
    for candle in candles:
        try:
            day = signal_service._candle_date(candle["time"])
        except (KeyError, TypeError, ValueError, OverflowError, OSError):
            continue
        if start_iso <= day < today_iso:
            out.append(candle)
    return out


def _window_candles(candles, period, today_iso, start_iso, sessions):
    """Nến của cửa sổ đo, theo thứ tự thời gian. Rỗng → caller bỏ mã.

    week  → `sessions` phiên đã đóng gần nhất; mã chưa đủ `sessions` phiên bị
            loại (kỳ ngắn hơn sẽ cho % không so sánh được với mã khác).
    month → dải theo lịch, có bao nhiêu lấy bấy nhiêu.
    """
    if period == "month":
        return _period_candles(candles, start_iso, today_iso)
    window = top_gain_service._recent_closed(candles, today_iso, sessions)
    return window if len(window) == sessions else []


def compute_period_gain(
    board_rows,
    history_candles,
    start=None,
    now=None,
    top_n=DEFAULT_TOP_N,
    period="week",
    sessions=RECENT_SESSIONS,
):
    """Hàm thuần: board_rows = rows view vn100 (symbol/price), history_candles =
    {mã: [nến ngày có volume]}. `start` chỉ dùng cho period='month' (ngày đầu kỳ
    lịch); period='week' tự lấy `sessions` phiên đã đóng gần nhất.

    Trả {rows, start, sessions} — `start` là ngày phiên ĐẦU cửa sổ thực tế (với
    week: phiên cũ nhất trong 5 phiên; None nếu không mã nào đủ dữ liệu)."""
    now = now or datetime.now(signal_service.VN_TZ)
    today_iso = now.date().isoformat()
    start_iso = start.isoformat() if isinstance(start, date) else (str(start)[:10] or None)
    scale = signal_service.PRICE_BOARD_SCALE

    rows = []
    window_start = None
    for row in board_rows or []:
        symbol = row.get("symbol")
        # Thanh khoản HIỆN TẠI (trong phiên) — vừa là bộ lọc, vừa là hệ số của
        # diem. Lọc trước khi đụng tới history: rẻ hơn và loại luôn phần
        # carry-over của rổ vn100 chưa khớp đủ 1 tỷ hôm nay.
        value_vnd = row.get("value") or 0
        if value_vnd <= top_gain_service.MIN_VALUE_VND:
            continue
        candles = (history_candles or {}).get(symbol)
        if not candles:
            continue
        window = _window_candles(candles, period, today_iso, start_iso, sessions)
        if not window:
            continue
        base_close = window[0].get("close")
        if not base_close or base_close <= 0:
            continue

        price = row.get("price") or 0
        gia_hien_tai = price / scale if price > 0 else window[-1].get("close", 0)
        if not gia_hien_tai or gia_hien_tai <= 0:
            continue
        pct = round((gia_hien_tai - base_close) / base_close * 100, 2)
        if pct <= 0:            # chỉ giữ mã tăng giá
            continue

        gia_tri_ty = value_vnd / _VND_TO_TY
        diem = pct * math.log10(gia_tri_ty + 1)

        if window_start is None:
            try:
                window_start = signal_service._candle_date(window[0]["time"])
            except (KeyError, TypeError, ValueError, OverflowError, OSError):
                window_start = None

        rows.append(
            {
                "symbol": symbol,
                "gia_tri_khop_lenh": round(gia_tri_ty, 3),
                "gia_hien_tai": round(gia_hien_tai, 2),
                "pct_tang": pct,
                "diem": round(diem, 2),
            }
        )

    rows.sort(key=lambda r: r["diem"], reverse=True)
    return {
        "rows": rows[:top_n],
        "start": start_iso if period == "month" else window_start,
        "sessions": sessions if period == "week" else None,
    }


def get_period_gain(period="week", top_n=DEFAULT_TOP_N):
    """Điểm gọi runtime: đọc rổ vn100 + nến base RAM."""
    history = getattr(signal_service, "_history_candles", None) or {}
    start = period_start(period) if period == "month" else None
    result = compute_period_gain(
        top_gain_service.board_rows(), history, start, top_n=top_n, period=period
    )
    result["period"] = period
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
