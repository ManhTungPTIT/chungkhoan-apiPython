"""Chart "TOP MÃ VƯỢT NỀN TÍCH LŨY 30 PHIÊN" — mã vừa bứt khỏi nền tích lũy.

Ý tưởng: một mã đi ngang 30 phiên trong biên độ hẹp là đang TÍCH LŨY. Phiên nào
giá vượt lên trên đỉnh của nền ĐÓ, kèm thanh khoản bật hẳn so với bình quân, thì
đó là điểm bứt phá đáng chú ý — chưa chạy xa nên còn chỗ vào.

Sáu bộ lọc (đều phải đạt):
    BiênĐộNền      <= 15    nền phải HẸP; nền rộng 40% thì "vượt đỉnh" vô nghĩa
    VượtNền        >= 0.5   đã thực sự thoát nền, không phải chạm đỉnh rồi thụt
    VượtNền        <= 5     VỪA thoát thôi — quá 5% là đã chạy, hết điểm vào
    TăngTừĐáy      <= 18    chưa bị kéo quá xa khỏi đáy nền
    TỷLệThanhKhoản >= 1.5   dòng tiền XÁC NHẬN cú vượt, không phải vượt trong im lặng
    GTGD           >= 1 tỷ  loại mã thanh khoản quá mỏng

Điểm xếp hạng:
    ĐiểmVịTrí  = max(0, 1 - |VượtNền - 2| / 3)      đỉnh tại VượtNền = 2%
    ĐiểmBậtNền = ĐiểmVịTrí × log10(TLTK + 1) × (1 - BiênĐộNền/100) × 100

ĐiểmVịTrí là hình tam giác chóp ở 2%: vượt 2% là "vừa đẹp", còn 0.5% (chưa chắc
đã thoát) hay 5% (đã chạy) đều bị hạ điểm. log10 để một mã thanh khoản gấp 20 lần
không đè bẹp toàn bảng. (1 - BiênĐộNền/100) thưởng nền càng hẹp càng tốt.

HAI QUY ƯỚC QUAN TRỌNG, đừng "sửa cho đúng" nếu chưa đọc:

1. "Thanh khoản" ở đây là TIỀN khớp lệnh (VND), không phải khối lượng — khớp chủ
   đề "dòng tiền xác nhận" và trùng quy ước TB20 của flow_surge_month_service.

2. Nền TB20 được NHÂN với hệ số khung giờ (flow_surge_service.baseline_bucket).
   Không có nó thì lúc 10h sáng GTGD mới chạy được ~1/5 phiên, TLTK >= 1.5 gần
   như không mã nào đạt và chart rỗng suốt buổi sáng. Cùng cách xử lý với chart
   "dòng tiền tăng đột biến".

Đọc CÙNG cache RAM như các chart khác (`market_board_full` +
`signal_service._history_candles`) → endpoint chỉ ĐỌC, không gọi vnstock.
"""

import math
from datetime import datetime

from app.services import flow_surge_service
from app.services import signal_service

# Nền tích lũy 30 phiên; thanh khoản so với TB 20 phiên (quy ước sẵn có của các
# chart dòng tiền). Hai cửa sổ KHÁC nhau là cố ý, không phải nhầm.
BASE_WINDOW = 30
AVG_WINDOW = 20

# Số bong bóng trên ảnh nền của chart — nhiều hơn cũng không có chỗ đặt.
DEFAULT_TOP_N = 15

MAX_BASE_RANGE_PCT = 15.0   # BiênĐộNền <=
MIN_BREAKOUT_PCT = 0.5      # VượtNền >=
MAX_BREAKOUT_PCT = 5.0      # VượtNền <=
MAX_GAIN_FROM_LOW_PCT = 18.0
MIN_LIQUIDITY_RATIO = 1.5
MIN_VALUE_VND = 1_000_000_000

# "Dòng tiền mạnh" của tile thống kê — mã thanh khoản gấp đôi nền trở lên.
STRONG_FLOW_RATIO = 2.0

# Chóp của ĐiểmVịTrí và nửa bề rộng tam giác.
SWEET_SPOT_PCT = 2.0
SWEET_SPOT_HALF_WIDTH = 3.0

_NGHIN_TO_VND = 1000
_VND_TO_TY = 1_000_000_000


def _base_candles(candles, today_iso, base_window):
    """`base_window` nến ĐÃ ĐÓNG gần nhất, bỏ nến hôm nay nếu nguồn đã trả về.

    "30 phiên trước" phải là nền QUÁ KHỨ: để lẫn nến hôm nay thì đỉnh nền tự nâng
    theo chính cú bứt phá đang xét, VượtNền co về ~0 và chart rỗng."""
    closed = candles
    if closed and signal_service._candle_date(closed[-1]["time"]) == today_iso:
        closed = closed[:-1]
    return closed[-base_window:] if base_window > 0 else []


def _baseline_value_vnd(candles, avg_window):
    """(TB giá trị khớp lệnh VND, số phiên hợp lệ) của `avg_window` phiên cuối.

    value phiên nền = volume × close × 1000 (close đơn vị nghìn đồng) — đúng
    nghĩa khớp lệnh, không gồm thỏa thuận. Nến thiếu volume/close bị bỏ."""
    values = []
    for candle in candles[-avg_window:] if avg_window > 0 else []:
        volume = candle.get("volume")
        close = candle.get("close")
        if volume and close:
            values.append(volume * close * _NGHIN_TO_VND)
    if not values:
        return None, 0
    return sum(values) / len(values), len(values)


def position_score(breakout_pct):
    """Tam giác chóp tại SWEET_SPOT_PCT, chạm 0 khi lệch quá SWEET_SPOT_HALF_WIDTH."""
    return max(0.0, 1.0 - abs(breakout_pct - SWEET_SPOT_PCT) / SWEET_SPOT_HALF_WIDTH)


def compute_base_breakout(
    board_rows,
    history_candles,
    now=None,
    top_n=DEFAULT_TOP_N,
    base_window=BASE_WINDOW,
    avg_window=AVG_WINDOW,
):
    """Hàm thuần → {rows, summary, ...}. Ngưỡng để nguyên hằng số module: chúng
    là ĐỊNH NGHĨA của chart, không phải tham số tinh chỉnh theo request.

    `rows` là top `top_n` theo ĐiểmBậtNền; `summary` tính trên TOÀN BỘ mã qua
    lọc — tile "MÃ VƯỢT NỀN: 48" phải đếm cả 48 chứ không phải 15 mã hiển thị."""
    now = now or datetime.now(signal_service.VN_TZ)
    today_iso = now.date().isoformat()
    scale = signal_service.PRICE_BOARD_SCALE
    time_bucket, factor = flow_surge_service.baseline_bucket(now)

    passed = []
    for row in board_rows or []:
        symbol = row.get("symbol")
        candles = (history_candles or {}).get(symbol)
        if not candles:
            continue

        base = _base_candles(candles, today_iso, base_window)
        # Đủ phiên nền — mã mới niêm yết chưa có nền để mà vượt.
        if len(base) < base_window:
            continue

        highs = [c["high"] for c in base if c.get("high")]
        lows = [c["low"] for c in base if c.get("low")]
        if not highs or not lows:
            continue
        base_high = max(highs)
        base_low = min(lows)
        if base_low <= 0 or base_high <= 0:
            continue

        price_raw = row.get("price") or 0
        price = price_raw / scale if price_raw > 0 else 0
        if price <= 0:
            continue

        base_range_pct = (base_high - base_low) / base_low * 100
        breakout_pct = (price - base_high) / base_high * 100
        gain_from_low_pct = (price - base_low) / base_low * 100

        value_vnd = row.get("value") or 0
        avg_vnd, session_count = _baseline_value_vnd(candles, avg_window)
        if not avg_vnd or avg_vnd <= 0 or session_count < avg_window:
            continue
        # Nền scale theo khung giờ — xem ghi chú đầu file.
        liquidity_ratio = value_vnd / (avg_vnd * factor)

        if base_range_pct > MAX_BASE_RANGE_PCT:
            continue
        if breakout_pct < MIN_BREAKOUT_PCT or breakout_pct > MAX_BREAKOUT_PCT:
            continue
        if gain_from_low_pct > MAX_GAIN_FROM_LOW_PCT:
            continue
        if liquidity_ratio < MIN_LIQUIDITY_RATIO:
            continue
        if value_vnd < MIN_VALUE_VND:
            continue

        diem = (
            position_score(breakout_pct)
            * math.log10(liquidity_ratio + 1)
            * (1 - base_range_pct / 100)
            * 100
        )

        passed.append(
            {
                "symbol": symbol,
                "gia_hien_tai": round(price, 2),
                "bien_do_nen": round(base_range_pct, 2),
                "vuot_nen": round(breakout_pct, 2),
                "tang_tu_day": round(gain_from_low_pct, 2),
                "tl_thanh_khoan": round(liquidity_ratio, 2),
                "gia_tri_khop_lenh": round(value_vnd / _VND_TO_TY, 3),
                "diem": round(diem, 2),
            }
        )

    passed.sort(key=lambda r: r["diem"], reverse=True)
    return {
        "rows": passed[:top_n],
        "summary": _summarize(passed),
        "base_window": base_window,
        "avg_window": avg_window,
        "time_bucket": time_bucket,
        "baseline_factor": factor,
    }


def _summarize(passed):
    """Năm ô thống kê ở đầu chart — tính trên TẤT CẢ mã qua lọc, không chỉ top."""
    count = len(passed)
    if count == 0:
        return {
            "count": 0,
            "total_gtgd_ty": 0.0,
            "avg_vuot_nen": 0.0,
            "avg_tl_thanh_khoan": 0.0,
            "strong_flow_count": 0,
        }
    return {
        "count": count,
        "total_gtgd_ty": round(sum(r["gia_tri_khop_lenh"] for r in passed), 1),
        "avg_vuot_nen": round(sum(r["vuot_nen"] for r in passed) / count, 2),
        "avg_tl_thanh_khoan": round(
            sum(r["tl_thanh_khoan"] for r in passed) / count, 2
        ),
        "strong_flow_count": sum(
            1 for r in passed if r["tl_thanh_khoan"] >= STRONG_FLOW_RATIO
        ),
    }


def get_base_breakout(top_n=DEFAULT_TOP_N):
    """Điểm gọi runtime: board TOÀN thị trường + nến base, đều từ cache RAM."""
    from app.data import market_cache

    board_rows = market_cache.get_snapshot("market_board_full") or []
    history = getattr(signal_service, "_history_candles", None) or {}

    result = compute_base_breakout(board_rows, history, top_n=top_n)
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
