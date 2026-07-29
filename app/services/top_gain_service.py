"""Chart "NHÓM TĂNG MẠNH NHẤT (NGẮN HẠN: T+N)" — top mã tăng mạnh nhất so với
nến N+1 phiên trước.

Đọc CÙNG nguồn cache như các view khác (view vn100 + `_history_candles` đã nạp
RAM) → endpoint chỉ ĐỌC, không gọi vnstock theo request.

Rổ mã = NGUYÊN rổ của view vn100: `vn100_service` đã lọc value > 1 tỷ và hợp
thêm mã đạt ngưỡng phiên gần nhất (carry-over, để đầu phiên danh sách không
rỗng). Service này KHÔNG lọc gì thêm — không lọc theo tín hiệu buy/hold nữa.

Mỗi mã 3 số cho chart combo:
  - gia_tri_khop_lenh (Tỷ): value khớp lệnh HÔM NAY, lấy live từ board (VND).
  - gia_hien_tai (Nghìn): giá live board / PRICE_BOARD_SCALE (fallback nến cuối).
  - pct_tang (%): (giá hiện tại − base) / base × 100.

`base` là TRUNG BÌNH `window` giá: close của (window − 1) phiên ĐÃ ĐÓNG gần nhất
cộng giá hiện tại, chia window. Nến hôm nay bị loại khỏi phần close (sau lần
refresh 15:05 nó đã nằm trong `_history_candles`, giữ lại sẽ đẩy cửa sổ lệch 1
phiên) — giá hôm nay đã vào base qua chính giá live. Với hôm nay 27/07 và các
phiên đã đóng 24/07, 23/07, 22/07, 21/07:
  window=2 → base = (close 24/07 + giá hiện tại) / 2
  window=3 → base = (close 23/07 + close 24/07 + giá hiện tại) / 3
  window=5 → base = (close 21/07..24/07 + giá hiện tại) / 5

Vì giá hiện tại nằm trong cả tử lẫn mẫu, biên độ % hẹp hơn cách so với một nến
mốc đơn lẻ. window=1 suy biến: base = chính giá hiện tại → 0%.

Xếp giảm dần theo pct_tang, cắt top N.
"""

from datetime import datetime

from app.services import signal_service

WINDOW = 2
DEFAULT_TOP_N = 20

_VND_TO_TY = 1_000_000_000


def _recent_closed(candles, today, count):
    """`count` nến ĐÃ ĐÓNG gần nhất (ngày < hôm nay), trả theo thứ tự thời gian
    tăng dần. Nến hỏng time → bỏ qua (không làm lệch phép đếm lùi vì cũng không
    đếm được). Trả ít hơn `count` nếu mã chưa đủ lịch sử.

    Quét NGƯỢC từ cuối và dừng sớm: lịch sử ~380 nến/mã × ~600 mã, chart chỉ cần
    vài nến cuối — quét xuôi toàn bộ làm radar (3 cửa sổ) treo hàng giây."""
    out = []
    for candle in reversed(candles):
        try:
            day = signal_service._candle_date(candle["time"])
        except (KeyError, TypeError, ValueError, OverflowError, OSError):
            continue
        if day >= today:
            continue
        out.append(candle)
        if len(out) == count:
            break
    out.reverse()
    return out


def compute_top_gain(board_rows, history_candles, now=None, top_n=DEFAULT_TOP_N, window=WINDOW):
    """Hàm thuần (dễ test): board_rows = rows view vn100, history_candles =
    {mã: [nến ngày...]}. Trả {rows, window}.

    `top_n=None` → KHÔNG cắt, trả cả rổ đã sort (tplus_wave_service dùng để tự
    cắt theo vùng radar)."""
    now = now or datetime.now(signal_service.VN_TZ)
    today = now.date().isoformat()
    scale = signal_service.PRICE_BOARD_SCALE
    window = max(int(window), 1)
    # (window − 1) close vào trung bình; vẫn lấy tối thiểu 1 nến để còn fallback
    # giá khi board thiếu `price`.
    need = max(window - 1, 1)

    rows = []
    for row in board_rows or []:
        symbol = row.get("symbol")
        candles = (history_candles or {}).get(symbol)
        if not candles:
            continue
        closed = _recent_closed(candles, today, need)
        if len(closed) < need:
            continue

        price = row.get("price") or 0
        gia_hien_tai = price / scale if price > 0 else closed[-1].get("close", 0)
        if not gia_hien_tai or gia_hien_tai <= 0:
            continue

        # `_recent_closed` trả tối đa `need` nến → với window ≥ 2 đây đúng là
        # (window − 1) close gần nhất; window = 1 thì trung bình chỉ có giá hiện tại.
        prev_closes = [c.get("close") for c in closed] if window > 1 else []
        if any(not close or close <= 0 for close in prev_closes):
            continue
        base = (sum(prev_closes) + gia_hien_tai) / window   # > 0 vì mọi số hạng > 0
        pct = round((gia_hien_tai - base) / base * 100, 2)

        rows.append(
            {
                "symbol": symbol,
                "gia_tri_khop_lenh": round((row.get("value") or 0) / _VND_TO_TY, 3),
                "gia_hien_tai": round(gia_hien_tai, 2),
                "pct_tang": pct,
            }
        )

    rows.sort(key=lambda r: r["pct_tang"], reverse=True)
    return {"rows": rows[:top_n], "window": window}


def board_rows():
    """Rổ mã của view vn100 (đã lọc value > 1 tỷ + carry-over ở vn100_service).
    Ưu tiên view trong market_wide (tươi ~20s) → fallback board_vn100 (~1 tiếng)
    — cùng thứ tự ưu tiên như endpoint /vn100. Cache chưa warm → []."""
    from app.data import market_cache

    wide = market_cache.get_snapshot("market_wide") or {}
    snap = market_cache.get_snapshot("board_vn100") or {}
    vn100_view = wide.get("vn100") or snap.get("vn100") or {}
    return vn100_view.get("data") or []


def get_top_gain(top_n=DEFAULT_TOP_N, window=WINDOW):
    """Điểm gọi runtime: đọc rổ vn100 + nến base RAM. Không gọi vnstock. Rỗng an
    toàn nếu cache chưa warm. window=2 (chart T+2) / 3 (chart T+3) — số phiên vào
    trung bình làm mốc so sánh."""
    history = getattr(signal_service, "_history_candles", None) or {}
    result = compute_top_gain(board_rows(), history, top_n=top_n, window=window)
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
