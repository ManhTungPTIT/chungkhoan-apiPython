"""Radar "Các mã đang có sóng tăng T+".

Mỗi trục = 1 mã đang có tín hiệu buy; 3 series = mức tăng cao nhất trong cửa sổ
T+2 / T+3 / T+5 phiên KỂ TỪ ngày báo buy (ngày báo không tính — khớp cột T+ trong
bảng bộ lọc, xem signal_service._trading_sessions_since).

Với mỗi mã:
  - T0  = close của nến NGÀY BÁO buy (từ signal_history / _history_candles).
  - T+N = N phiên giao dịch đầu tiên SAU ngày báo.
  - gia_tang_TN = (max(high của T+1..T+N) − close_T0) / close_T0 × 100.

Vì tính theo %, đơn vị giá của nến (nghìn đồng) không ảnh hưởng.
Xếp giảm dần theo gia_tang_T5, lấy top N mã (mặc định 15) để radar đọc được.
"""

from app.services import signal_service

WINDOWS = (2, 3, 5)
MAX_WINDOWS = 6      # tối đa số cửa sổ (vùng) — hợp bảng màu radar
WINDOW_MIN = 1
WINDOW_MAX = 30
DEFAULT_TOP_N = 15
# Số mã mỗi VÙNG của radar 3 vùng (mỗi cửa sổ T+ một vùng góc riêng, top mã
# sort theo chính cửa sổ đó — một mã có thể xuất hiện ở nhiều vùng).
DEFAULT_ZONE_N = 8


def parse_windows(raw, default=WINDOWS):
    """Chuỗi 'windows' từ query (vd '2,4,7') → tuple int đã lọc/dedupe/sort. Chỉ
    giữ [WINDOW_MIN..WINDOW_MAX], tối đa MAX_WINDOWS cửa sổ. Rỗng/rác → default."""
    if not raw:
        return default
    seen = []
    for part in str(raw).split(","):
        part = part.strip()
        try:
            n = int(part)
        except (TypeError, ValueError):
            continue
        if WINDOW_MIN <= n <= WINDOW_MAX and n not in seen:
            seen.append(n)
    if not seen:
        return default
    return tuple(sorted(seen)[:MAX_WINDOWS])


def _wave_for_symbol(candles, signal_date, windows=WINDOWS):
    """Tính {t2,t3,t5} cho 1 mã. candles: list nến tăng dần theo time (epoch giây),
    mỗi nến có high/close. signal_date: ISO 'YYYY-MM-DD' ngày báo buy.

    Trả None nếu không tìm được nến ngày báo (T0) hoặc close_T0 <= 0 (không tính %
    được) — caller bỏ mã đó. Cửa sổ thiếu phiên (mã mới buy chưa đủ N phiên): dùng
    các phiên có sẵn; chưa có phiên nào sau ngày báo → 0."""
    t0_close = None
    after = []  # nến các phiên SAU ngày báo, theo thứ tự
    for candle in candles:
        try:
            day = signal_service._candle_date(candle["time"])
        except (KeyError, TypeError, ValueError, OverflowError, OSError):
            continue
        if day == signal_date:
            t0_close = candle.get("close")
        elif t0_close is not None and day > signal_date:
            after.append(candle)
    if not t0_close or t0_close <= 0:
        return None

    result = {}
    for n in windows:
        window = after[:n]
        highs = [c["high"] for c in window if c.get("high") is not None]
        if highs:
            result[f"t{n}"] = round((max(highs) - t0_close) / t0_close * 100, 2)
        else:
            result[f"t{n}"] = 0.0
    return result


def compute_tplus_wave(
    signals, history_candles, top_n=DEFAULT_TOP_N, windows=WINDOWS, zone_n=DEFAULT_ZONE_N
):
    """Hàm thuần (dễ test): nhận dict signals {mã: {signal,date,...}} + dict
    history_candles {mã: [nến...]} → dict kết quả cho FE.

    Ngoài shape cũ (symbols/series/max_value — 3 series trên CÙNG bộ trục), trả
    thêm `zones` cho radar 3 VÙNG: mỗi cửa sổ có top `zone_n` mã RIÊNG sort theo
    chính cửa sổ đó (chỉ mức tăng > 0 — vùng là TOP TĂNG), một mã có thể xuất
    hiện ở nhiều vùng."""
    rows = []
    for symbol, entry in (signals or {}).items():
        if not isinstance(entry, dict) or entry.get("signal") != "buy":
            continue
        candles = (history_candles or {}).get(symbol)
        if not candles:
            continue
        signal_date = str(entry.get("date") or "")[:10]
        if not signal_date:
            continue
        wave = _wave_for_symbol(candles, signal_date, windows)
        if wave is None:
            continue
        rows.append({"symbol": symbol, **wave})

    # Sóng mạnh nhất lên đầu; cắt top N để radar không quá nhiều trục.
    rows.sort(key=lambda r: r.get(f"t{windows[-1]}", 0), reverse=True)
    top = rows[:top_n]

    symbols = [r["symbol"] for r in top]
    series = {f"t{n}": [r[f"t{n}"] for r in top] for n in windows}
    all_values = [v for n in windows for v in series[f"t{n}"]]
    max_value = round(max(all_values), 2) if all_values else 0.0

    # Vùng theo từng cửa sổ: lấy trên TOÀN BỘ rows (không phải top đã cắt theo
    # t5) để mã mạnh riêng ở cửa sổ ngắn không bị sót.
    zones = {}
    for n in windows:
        key = f"t{n}"
        gainers = sorted(
            (r for r in rows if r.get(key, 0) > 0), key=lambda r: r[key], reverse=True
        )[:zone_n]
        zones[key] = {
            "symbols": [r["symbol"] for r in gainers],
            "values": [r[key] for r in gainers],
        }
        all_values.extend(zones[key]["values"])
    max_value = round(max(all_values), 2) if all_values else 0.0

    return {"symbols": symbols, "series": series, "max_value": max_value, "zones": zones}


def get_tplus_wave(top_n=DEFAULT_TOP_N, windows=WINDOWS, zone_n=DEFAULT_ZONE_N):
    """Điểm gọi runtime: đọc cache tín hiệu + nến base đã nạp sẵn trong RAM
    (signal_service làm mới theo scheduler). `windows` = các cửa sổ T+ muốn xem."""
    cache = getattr(signal_service, "_cache", None) or {}
    signals = cache.get("signals") or {}
    history = getattr(signal_service, "_history_candles", None) or {}
    result = compute_tplus_wave(signals, history, top_n=top_n, windows=windows, zone_n=zone_n)
    result["windows"] = list(windows)
    result["generated_at"] = cache.get("last_refresh")
    return result
