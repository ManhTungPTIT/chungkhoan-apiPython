"""Radar "BẢN ĐỒ SỨC MẠNH TĂNG GIÁ CỔ PHIẾU" (Các mã đang có sóng tăng T+).

Dùng CHUNG một định nghĩa với hai chart "NHÓM TĂNG MẠNH NHẤT (NGẮN HẠN: T+N)":
service này chỉ gọi lại `top_gain_service.compute_top_gain` cho từng cửa sổ rồi
xếp kết quả thành các VÙNG của radar. Nhờ vậy vùng T+2 / T+3 của radar luôn là
top của đúng bảng xếp hạng mà hai chart kia hiển thị, T+5 chạy cùng công thức
chỉ khác độ lùi.

Nhắc lại định nghĩa (chi tiết ở top_gain_service):
  - Rổ mã = nguyên rổ view vn100 (đã lọc value > 1 tỷ + carry-over).
  - t{N} = (giá hiện tại − close nến đã đóng lùi N+1 phiên) / close đó × 100.

Vì tính theo %, đơn vị giá của nến (nghìn đồng) không ảnh hưởng.
Mỗi vùng lấy top `zone_n` mã sort theo CHÍNH cửa sổ đó, chỉ giữ mức tăng > 0 —
một mã có thể xuất hiện ở nhiều vùng.
"""

from app.services import signal_service, top_gain_service

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


def compute_tplus_wave(
    board_rows,
    history_candles,
    now=None,
    top_n=DEFAULT_TOP_N,
    windows=WINDOWS,
    zone_n=DEFAULT_ZONE_N,
):
    """Hàm thuần (dễ test): board_rows = rows view vn100, history_candles =
    {mã: [nến ngày...]} → dict kết quả cho FE.

    `zones` là thứ radar thật sự vẽ: mỗi cửa sổ một vùng góc, top `zone_n` mã
    RIÊNG của cửa sổ đó. `symbols`/`series`/`max_value` là shape cũ (3 series
    trên CÙNG bộ trục) — giữ cho FE fallback và cho check-rỗng của marketCharts."""
    # Xếp hạng đầy đủ theo TỪNG cửa sổ, đúng thứ tự mà chart T+N hiển thị.
    ranked = {
        n: top_gain_service.compute_top_gain(
            board_rows, history_candles, now=now, top_n=None, window=n
        )["rows"]
        for n in windows
    }

    # Gộp lại theo mã cho shape cũ. Mã vắng ở một cửa sổ (chưa đủ nến để lùi sâu
    # hơn) → 0.0 ở cửa sổ đó.
    by_symbol = {}
    for n in windows:
        for row in ranked[n]:
            by_symbol.setdefault(row["symbol"], {})[f"t{n}"] = row["pct_tang"]
    rows = [
        {"symbol": symbol, **{f"t{n}": pcts.get(f"t{n}", 0.0) for n in windows}}
        for symbol, pcts in by_symbol.items()
    ]

    # Sóng mạnh nhất (theo cửa sổ dài nhất) lên đầu; cắt top N để radar cũ không
    # quá nhiều trục.
    rows.sort(key=lambda r: r.get(f"t{windows[-1]}", 0), reverse=True)
    top = rows[:top_n]

    symbols = [r["symbol"] for r in top]
    series = {f"t{n}": [r[f"t{n}"] for r in top] for n in windows}
    all_values = [v for n in windows for v in series[f"t{n}"]]

    # Vùng theo từng cửa sổ: lấy trên xếp hạng ĐẦY ĐỦ (không phải top đã cắt
    # theo cửa sổ dài nhất) để mã mạnh riêng ở cửa sổ ngắn không bị sót.
    zones = {}
    for n in windows:
        key = f"t{n}"
        gainers = [r for r in ranked[n] if r["pct_tang"] > 0][:zone_n]
        zones[key] = {
            "symbols": [r["symbol"] for r in gainers],
            "values": [r["pct_tang"] for r in gainers],
        }
        all_values.extend(zones[key]["values"])
    max_value = round(max(all_values), 2) if all_values else 0.0

    return {"symbols": symbols, "series": series, "max_value": max_value, "zones": zones}


def get_tplus_wave(top_n=DEFAULT_TOP_N, windows=WINDOWS, zone_n=DEFAULT_ZONE_N):
    """Điểm gọi runtime: đọc rổ vn100 + nến base đã nạp sẵn trong RAM (cùng
    nguồn với chart T+2/T+3). `windows` = các cửa sổ T+ muốn xem."""
    history = getattr(signal_service, "_history_candles", None) or {}
    result = compute_tplus_wave(
        top_gain_service.board_rows(), history, top_n=top_n, windows=windows, zone_n=zone_n
    )
    result["windows"] = list(windows)
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
