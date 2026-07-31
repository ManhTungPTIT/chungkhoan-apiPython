"""Chart "NHÓM TĂNG MẠNH NHẤT (NGẮN HẠN: T+N)" — top mã mạnh nhất so với nến
đã đóng lùi N+1 phiên, xếp hạng theo ĐIỂM (tăng giá có trọng số thanh khoản).

Đọc CÙNG nguồn cache như các view khác (view vn100 + `_history_candles` đã nạp
RAM) → endpoint chỉ ĐỌC, không gọi vnstock theo request.

Rổ mã = rổ của view vn100 (`vn100_service` đã lọc value > 1 tỷ + carry-over mã
đạt ngưỡng phiên gần nhất), lọc thêm hai điều kiện của chart:
  - thanh khoản HÔM NAY > 1 tỷ — chặn phần carry-over chưa khớp đủ 1 tỷ hôm nay;
  - giá tăng (pct_tang > 0).

Mỗi mã 4 số cho chart combo:
  - gia_tri_khop_lenh (Tỷ): value khớp lệnh HÔM NAY, lấy live từ board (VND).
  - gia_hien_tai (Nghìn): giá live board / PRICE_BOARD_SCALE (fallback nến cuối).
  - pct_tang (%): (giá hiện tại − close mốc) / close mốc × 100 — số HIỂN THỊ ở
    cột xanh.
  - diem: pct_tang × log10(thanh khoản Tỷ + 1) — số dùng để XẾP HẠNG.

`base` = close của nến ĐÃ ĐÓNG lùi (window + 1) phiên, tức `closed[-(window+1)]`.
Convention N+1 là lựa chọn của người dùng: "nến 2 phiên trước" = có 2 phiên nằm
giữa nó và hôm nay. Nến hôm nay bị loại khỏi phép đếm lùi (sau lần refresh 15:05
nó đã nằm trong `_history_candles`, giữ lại sẽ đẩy mốc lệch đúng 1 phiên giữa
trong phiên và sau phiên). Với hôm nay 31/07 và các phiên đã đóng 30, 29, 28, 27:
  window=2 → mốc = close 29/07
  window=3 → mốc = close 28/07
  window=5 → mốc = close 24/07 (lùi 6 phiên)

Hệ số log10 tính theo TỶ đồng, không phải VND. Theo VND mọi mã đều rơi vào
log10(1e9…1e12) = 9…12, hệ số gần bằng nhau → xếp hạng thoái hoá về đúng thứ tự
% tăng, thanh khoản coi như không tính. Theo tỷ hệ số trải 0.3 (1 tỷ) → 3 (1000
tỷ), đủ để mã tăng ít nhưng dòng tiền lớn vượt mã tăng nhiều mà thanh khoản mỏng.
Hai bộ lọc trên đảm bảo hệ số > 0 và pct > 0 nên diem luôn dương, không có
nghịch lý "giảm sâu + thanh khoản lớn = điểm cao".

Xếp giảm dần theo diem, cắt top N.
"""

import math
from datetime import datetime

from app.services import signal_service

WINDOW = 2
DEFAULT_TOP_N = 20

_VND_TO_TY = 1_000_000_000
# Thanh khoản tối thiểu hôm nay để vào bảng (VND).
MIN_VALUE_VND = 1_000_000_000


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
    need = window + 1   # nến mốc là closed[-(window + 1)] → cần đúng bấy nhiêu nến

    rows = []
    for row in board_rows or []:
        symbol = row.get("symbol")
        value_vnd = row.get("value") or 0
        if value_vnd <= MIN_VALUE_VND:
            continue
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

        # `_recent_closed` trả đúng `need` nến theo thứ tự thời gian tăng dần →
        # phần tử ĐẦU chính là nến lùi (window + 1) phiên.
        base = closed[0].get("close") or 0
        if base <= 0:
            continue
        pct = (gia_hien_tai - base) / base * 100
        if pct <= 0:            # chỉ giữ mã tăng giá
            continue

        gia_tri_khop_lenh = value_vnd / _VND_TO_TY
        diem = pct * math.log10(gia_tri_khop_lenh + 1)

        rows.append(
            {
                "symbol": symbol,
                "gia_tri_khop_lenh": round(gia_tri_khop_lenh, 3),
                "gia_hien_tai": round(gia_hien_tai, 2),
                "pct_tang": round(pct, 2),
                "diem": round(diem, 2),
            }
        )

    rows.sort(key=lambda r: r["diem"], reverse=True)
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
    toàn nếu cache chưa warm. window=2 (chart T+2) / 3 (chart T+3) — nến mốc là
    phiên đã đóng lùi (window + 1)."""
    history = getattr(signal_service, "_history_candles", None) or {}
    result = compute_top_gain(board_rows(), history, top_n=top_n, window=window)
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
