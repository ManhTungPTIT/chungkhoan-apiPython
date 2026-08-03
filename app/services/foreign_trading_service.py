"""Chart "GIAO DỊCH KHỐI NGOẠI 30 PHIÊN GẦN NHẤT" — tổng mua/bán/ròng khối
ngoại TOÀN thị trường, mỗi phiên một dòng.

Dữ liệu đến từ HAI đường, cố ý tách bạch:

1. Phiên HÔM NAY: gộp `foreign_buy_value`/`foreign_sell_value` của mọi mã trong
   snapshot `market_board_full` (0 request thêm — market_refresher đã fetch board
   cho các chart khác). Đây là đường duy nhất có số liệu trong phiên.

2. Backfill 29 phiên TRƯỚC: cộng `Trading(symbol=…).foreign_trade()` của từng mã
   rồi gộp theo ngày. Nặng (~1.000 request) nên chạy nền tối đa 1 lần/ngày và chỉ
   khi lịch sử đang thủng — xem `needs_backfill`.

Bản trước đây gọi `Trading(source="VCI").foreign_trade()` KHÔNG truyền mã; lớp
Trading mặc định symbol='VCI' nên nó trả về khối ngoại của riêng cổ phiếu VCI
(~32 tỷ/phiên) chứ không phải toàn thị trường (~1.900 tỷ/phiên) — lệch ~100 lần.
Phải truyền symbol tường minh, và phải CỘNG cả rổ.

Hai đường trên cộng trên cùng một universe (toàn bộ mã niêm yết) nên giá trị
liền mạch qua ranh giới backfill ↔ tích lũy hằng ngày.
"""

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from app.data import market_cache

VN_TZ = timezone(timedelta(hours=7))
RUNTIME_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
)
FOREIGN_TRADING_HISTORY_CACHE_FILE = os.environ.get(
    "FOREIGN_TRADING_HISTORY_CACHE_FILE",
    os.path.join(RUNTIME_DATA_DIR, "foreign_trading_history_cache.json"),
)
MAX_HISTORY_LENGTH = 30
# Cùng mức song song với refresh_sector_flow (luồng ~1.600 request/ngày đã chạy
# ổn định với 8 worker).
BACKFILL_WORKERS = 8
# …nhưng số luồng KHÔNG phải thứ giới hạn nhịp: đo 01/08/2026, 8 luồng không bóp
# chạy ~800 request/phút và chết ở mã thứ ~500 với "Rate limit exceeded"
# (vnstock Golden: 500 request/phút cho TOÀN app). Bóp về 200/phút để vừa không
# chạm trần vừa chừa hạn mức cho các luồng refresh khác. 1.575 mã ≈ 8 phút.
BACKFILL_RATE_PER_MIN = 200
# Hỏng quá tỉ lệ này thì BỎ cả bản quét thay vì cộng thiếu — xem
# fetch_history_from_vendor.
MAX_BACKFILL_FAILURE_RATE = 0.1
# Lịch sử đủ 30 dòng mà dòng mới nhất cũ hơn ngần này ngày lịch thì coi là thủng
# (server tắt vài hôm) và quét lại. 4 ngày phủ được T7+CN kèm một ngày lễ.
STALE_HISTORY_DAYS = 4

_backfill_date = None  # ngày (giờ VN) đã backfill xong, đọc lại từ đĩa khi boot


def _history_item(date_str, buy_value, sell_value, net_value, total_value):
    return {
        "date": date_str,
        "buy_value": int(buy_value),
        "sell_value": int(sell_value),
        "net_value": int(net_value),
        "total_value": int(total_value),
    }


def _item_from_totals(date_str, buy_value, sell_value):
    return _history_item(
        date_str, buy_value, sell_value, buy_value - sell_value, buy_value + sell_value
    )


def is_session_day(date_str) -> bool:
    """T2–T6. Ngày lễ vẫn lọt (không có lịch nghỉ), nhưng chốt chặn trùng dòng
    trong `update_history_from_board` bắt nốt trường hợp đó."""
    try:
        return date.fromisoformat(str(date_str)[:10]).weekday() < 5
    except ValueError:
        return False


def _save_history_cache(history):
    try:
        with open(FOREIGN_TRADING_HISTORY_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {"history": history, "backfill_date": _backfill_date}, f, ensure_ascii=False
            )
    except OSError:
        pass


def load_history_cache():
    global _backfill_date
    try:
        with open(FOREIGN_TRADING_HISTORY_CACHE_FILE, encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, ValueError):
        return []

    if not isinstance(payload, dict):
        return []

    history = payload.get("history")
    if not isinstance(history, list):
        return []

    saved_date = payload.get("backfill_date")
    _backfill_date = saved_date if isinstance(saved_date, str) else None

    validated = []
    for item in history:
        if not isinstance(item, dict):
            continue
        item_date = item.get("date")
        if not isinstance(item_date, str) or not item_date:
            continue
        buy_value = int(item.get("buy_value") or 0)
        sell_value = int(item.get("sell_value") or 0)
        net_value = int(item.get("net_value") or 0)
        total_value = int(item.get("total_value") or 0)
        validated.append(_history_item(item_date, buy_value, sell_value, net_value, total_value))

    market_cache.set_snapshot("foreign_trading_history", validated)
    return validated


def get_history():
    history = market_cache.get_snapshot("foreign_trading_history")
    if history is None:
        return load_history_cache()
    return history


# ─── Backfill từ vendor: cộng khối ngoại từng mã ──────────────────────────────


def _symbol_foreign_history(symbol):
    """{ngày: (mua, bán)} của MỘT mã, hoặc None nếu GỌI HỎNG.

    Phân biệt None (hỏng) với {} (mã không có giao dịch khối ngoại) là bắt buộc:
    tổng chỉ đúng khi gần như cả rổ trả về được, mà rổ có hàng trăm mã trắng
    hợp lệ nên không thể lấy "rỗng" làm dấu hiệu hỏng — xem `fetch_history_from_vendor`.

    Symbol PHẢI truyền qua constructor: `foreign_trade(symbol=…)` nhận tham số
    nhưng bỏ qua, vẫn trả về mã mặc định của lớp Trading (='VCI') — chính là bẫy
    đã làm bản trước lấy nhầm dữ liệu một mã thay cho cả thị trường.

    Bắt cả BaseException: chạm rate limit thì vnstock in cảnh báo rồi raise
    SystemExit, để lọt là chết luôn cả tiến trình API."""
    try:
        from vnstock_data import Trading

        df = Trading(symbol=symbol, source="VCI").foreign_trade()
    except BaseException:  # noqa: BLE001 — cố ý bắt cả SystemExit
        return None

    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return {}
    if "trading_date" not in df.columns:
        return {}

    # KHỚP LỆNH, không phải `_total`: dòng hôm nay gộp từ price_board, mà cột
    # foreign_buy_value ở đó nằm trong nhóm "match" = chỉ khớp lệnh. Lấy `_total`
    # (khớp lệnh + thỏa thuận) là mọi phiên lịch sử cao hơn dòng hôm nay một cách
    # có hệ thống. Đo 31/07/2026 trên 60 mã lớn nhất: thỏa thuận 44/2.314 tỷ
    # (~2%) — nhỏ, nhưng là độ lệch một chiều nên vẫn đáng bỏ.
    buy_col = "fr_buy_value_matched" if "fr_buy_value_matched" in df.columns else "fr_buy_value_total"
    sell_col = "fr_sell_value_matched" if "fr_sell_value_matched" in df.columns else "fr_sell_value_total"

    out = {}
    for _, row in df.iterrows():
        day = row.get("trading_date")
        if pd.isna(day):
            continue
        buy_value = row.get(buy_col)
        sell_value = row.get(sell_col)
        buy_value = 0.0 if pd.isna(buy_value) else float(buy_value or 0)
        sell_value = 0.0 if pd.isna(sell_value) else float(sell_value or 0)
        out[str(day)[:10]] = (buy_value, sell_value)
    return out


class _Pacer:
    """Giữ nhịp gọi vendor dưới `per_min` request/phút cho TẤT CẢ luồng.

    Đặt mốc phát tiếp theo trong khoá rồi mới sleep NGOÀI khoá: sleep bên trong
    thì các luồng xếp hàng nối đuôi và nhịp thực tế tụt xuống còn 1 luồng."""

    def __init__(self, per_min):
        self._gap = 60.0 / per_min if per_min and per_min > 0 else 0.0
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self):
        if self._gap <= 0:
            return
        with self._lock:
            start_at = max(time.monotonic(), self._next_at)
            self._next_at = start_at + self._gap
        delay = start_at - time.monotonic()
        if delay > 0:
            time.sleep(delay)


def fetch_history_from_vendor(
    symbols,
    limit_days=MAX_HISTORY_LENGTH,
    workers=BACKFILL_WORKERS,
    rate_per_min=BACKFILL_RATE_PER_MIN,
):
    """Lịch sử khối ngoại TOÀN thị trường = tổng theo ngày của cả rổ `symbols`.

    Mỗi mã trả sẵn ~100 phiên trong MỘT request nên cả 30 phiên chỉ tốn đúng một
    lượt quét — không lặp theo ngày. `rate_per_min<=0` tắt bóp nhịp (dùng cho test)."""
    symbols = [s for s in (symbols or []) if isinstance(s, str) and s]
    if not symbols:
        return []

    pacer = _Pacer(rate_per_min)

    def fetch(symbol):
        pacer.wait()
        return _symbol_foreign_history(symbol)

    totals = {}
    failed = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for per_symbol in pool.map(fetch, symbols):
            if per_symbol is None:
                failed += 1
                continue
            for day, (buy_value, sell_value) in per_symbol.items():
                bucket = totals.setdefault(day, [0.0, 0.0])
                bucket[0] += buy_value
                bucket[1] += sell_value

    # Quét thiếu thì THÀ KHÔNG CÓ còn hơn có số sai: đây là phép CỘNG cả rổ, mất
    # 30% số mã là mọi phiên thấp đi 30% một cách âm thầm — trên chart nó vẫn là
    # những cột trông hợp lý. Đúng kịch bản xảy ra khi chạm rate limit giữa chừng:
    # các mã còn lại hỏng hàng loạt.
    if failed > len(symbols) * MAX_BACKFILL_FAILURE_RATE:
        return []

    days = sorted(totals)[-limit_days:]
    return [_item_from_totals(day, totals[day][0], totals[day][1]) for day in days]


def merge_backfill(existing, backfill, limit=MAX_HISTORY_LENGTH):
    """Trộn lịch sử đang có với bản backfill. Hàm thuần (dễ test).

    Dòng ĐANG CÓ được ưu tiên giữ vì nó gộp từ board — cùng universe, cùng thời
    điểm chốt với dòng hôm nay, nên liền mạch hơn số vendor. Nhưng chỉ giữ khi:

      - ngày đó CÓ trong bản vendor  → vendor xác nhận đúng là một phiên, hoặc
      - ngày đó MỚI HƠN phiên cuối vendor và rơi vào T2–T6 → phiên vendor chưa
        kịp công bố (hôm nay).

    Nhánh thứ hai là chỗ dọn rác: dòng cuối tuần do bản cũ ghi nhầm (board thứ
    Sáu còn trong RAM, xem `update_history_from_board`) mới hơn phiên cuối vendor
    nhưng rơi vào T7 → bị loại."""
    backfill = [item for item in (backfill or []) if isinstance(item, dict) and item.get("date")]
    if not backfill:
        return list(existing or [])[-limit:]

    by_date = {item["date"]: item for item in backfill}
    vendor_days = set(by_date)
    newest_vendor = max(vendor_days)

    for item in existing or []:
        if not isinstance(item, dict):
            continue
        item_date = item.get("date")
        if not isinstance(item_date, str):
            continue
        if item_date in vendor_days or (item_date > newest_vendor and is_session_day(item_date)):
            by_date[item_date] = item

    return [by_date[day] for day in sorted(by_date)[-limit:]]


def needs_backfill(now=None) -> bool:
    """Có nên chạy lượt quét ~1.000 request hôm nay không?

    Tối đa 1 lần/ngày, VÀ chỉ khi lịch sử đang thủng: chưa đủ 30 phiên, hoặc
    dòng mới nhất quá cũ (server tắt vài hôm). Lịch sử lành thì đường tích lũy
    từ board đã đủ nuôi chart, quét lại mỗi ngày là phí request."""
    now = now or datetime.now(VN_TZ)
    if _backfill_date == now.date().isoformat():
        return False

    history = get_history() or []
    if len(history) < MAX_HISTORY_LENGTH:
        return True

    try:
        newest = date.fromisoformat(history[-1]["date"][:10])
    except (KeyError, TypeError, ValueError):
        return True
    return (now.date() - newest).days > STALE_HISTORY_DAYS


def apply_backfill(rows, now=None) -> bool:
    """Trộn bản backfill vào lịch sử rồi ghi đĩa. Đánh dấu đã quét hôm nay CHỈ
    khi thành công → lượt refresh sau thử lại thay vì đợi sang ngày mới."""
    global _backfill_date
    if not rows:
        return False
    merged = merge_backfill(get_history() or [], rows)
    if not merged:
        return False
    _backfill_date = (now or datetime.now(VN_TZ)).date().isoformat()
    market_cache.set_snapshot("foreign_trading_history", merged)
    _save_history_cache(merged)
    return True


# ─── Đường tích lũy hằng ngày từ board ────────────────────────────────────────


def aggregate_foreign_totals(board_rows, now=None):
    now = now or datetime.now(VN_TZ)
    today = now.date().isoformat()
    if not isinstance(board_rows, list):
        return _history_item(today, 0, 0, 0, 0)

    buy_value = 0
    sell_value = 0
    for row in board_rows:
        if not isinstance(row, dict):
            continue
        buy_value += int(row.get("foreign_buy_value") or 0)
        sell_value += int(row.get("foreign_sell_value") or 0)

    return _item_from_totals(today, buy_value, sell_value)


def build_current_summary(board_rows):
    return aggregate_foreign_totals(board_rows)


def get_current_summary():
    board_rows = market_cache.get_snapshot("market_board_full")
    if isinstance(board_rows, list):
        return aggregate_foreign_totals(board_rows)

    history = get_history()
    if history:
        return history[-1]

    return _history_item(datetime.now(VN_TZ).date().isoformat(), 0, 0, 0, 0)


def update_history_from_board(board_rows, now=None):
    summary = aggregate_foreign_totals(board_rows, now=now)
    if summary["buy_value"] == 0 and summary["sell_value"] == 0:
        return False

    # T7/CN không có phiên, nhưng snapshot board cuối phiên thứ Sáu vẫn nằm
    # nguyên trong RAM và vẫn có value > 0 nên qua được chốt preopen của
    # market_refresher — ghi tiếp là đẻ ra một "phiên" cuối tuần trùng khít
    # thứ Sáu (đúng lỗi đã thấy trên chart: 01/08 T7 = bản sao của 31/07).
    if not is_session_day(summary["date"]):
        return False

    history = get_history() or []
    existing_index = next(
        (index for index, item in enumerate(history) if item.get("date") == summary["date"]),
        None,
    )

    if existing_index is not None:
        history[existing_index] = summary
    elif _is_replay_of_last(history, summary):
        return False
    else:
        history.append(summary)
        history = history[-MAX_HISTORY_LENGTH:]

    market_cache.set_snapshot("foreign_trading_history", history)
    _save_history_cache(history)
    return True


def _is_replay_of_last(history, summary) -> bool:
    """Ngày mới nhưng tổng trùng ĐẾN TỪNG ĐỒNG với dòng cuối → vẫn là board của
    phiên trước (ngày lễ, hoặc trước giờ mở phiên hôm sau), không phải phiên mới.

    Ở thang nghìn tỷ, hai phiên khác nhau trùng nhau tới đơn vị đồng trên CẢ hai
    chiều mua và bán là chuyện không xảy ra, nên chốt này an toàn."""
    if not history:
        return False
    last = history[-1]
    return (
        last.get("buy_value") == summary["buy_value"]
        and last.get("sell_value") == summary["sell_value"]
    )
