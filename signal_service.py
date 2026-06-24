"""Tín hiệu mua/bán Trend (EMA20 + MACD) cho rổ VN100 — tính nền, cache file.

Nến *ngày* chỉ đổi tối đa 1 lần/phiên (lúc đóng cửa) nên lịch sử được fetch chậm
(throttle) 1 lần/ngày, tính tín hiệu cuối mỗi mã rồi cache ra JSON. Endpoint
/vn100 merge sẵn field `signal` từ cache → FE chỉ đọc item.signal (panel đã hỗ trợ),
không phải fetch 100 mã → né rate limit.

Logic port từ chart/src/feature/chart/untils/indicators.js (generateSignals):
vào lệnh khi close > EMA20 VÀ MACD > Signal; ra lệnh khi close < EMA20 VÀ
MACD < Signal. (JS đặt tên "ma20" nhưng gọi calcEMA → thực chất là EMA20.)
"""

import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone

import data_source
import vn100_service

logger = logging.getLogger(__name__)

VN_TZ = timezone(timedelta(hours=7))
HISTORY_START = "2025-01-01"


def _resolve_cache_file():
    """Đường dẫn cache runtime. Cho phép trỏ sang volume bền qua env
    SIGNAL_CACHE_FILE (môi trường read-only/ephemeral) — mặc định cạnh module."""
    return os.environ.get("SIGNAL_CACHE_FILE") or os.path.join(
        os.path.dirname(__file__), "signal_cache.json"
    )


CACHE_FILE = _resolve_cache_file()
# Seed commit sẵn (KHÔNG gitignore): cold start chưa có cache runtime vẫn có
# signal hiển thị ngay, khỏi đợi warm refresh. Tín hiệu nến-ngày nên seed cũ vẫn
# đúng tới 15:05; warm refresh sau đó tự cập nhật.
SEED_FILE = os.path.join(os.path.dirname(__file__), "signal_cache.seed.json")
# Giãn cách giữa mỗi mã. Gói Community vnstock giới hạn 60 request/phút, nên
# throttle phải ≥1.0s (≤60/phút); 1.1s (~54/phút) để có biên an toàn — nếu nhanh
# hơn, một phần mã bị rate-limit mỗi lần refresh. ~100 mã ≈ 110s/lần.
THROTTLE_S = 1.1
REFRESH_HOUR = 15  # ~15:05 giờ VN, sau khi phiên đóng cửa
REFRESH_MINUTE = 5

# Retry vòng 2 cho mã fetch lỗi (thường do rate-limit) — backoff luỹ thừa.
RETRY_COOLDOWN_S = 60  # nghỉ 1 lần giữa vòng 1 và vòng 2 cho rate-limit hồi
RETRY_BASE_S = 1.5  # giãn cách cơ bản ở vòng 2
RETRY_BACKOFF_FACTOR = 2  # mỗi lần lỗi liên tiếp ×2
RETRY_MAX_BACKOFF_S = 30  # trần delay

# Mã fetch lỗi cả 2 vòng (rate-limit dai dẳng) được retry nền mỗi phút tới khi
# fetch được, thay vì phải đợi lịch ~15:05 hôm sau.
RETRY_INTERVAL_S = 60

# Cache nội bộ: {"last_refresh": "YYYY-MM-DD", "signals": {symbol: {signal, date, price}}}
_cache = {"last_refresh": None, "signals": {}}
# Khối lượng khớp phiên gần nhất {symbol: volume} của MỌI mã fetch được (kể cả mã
# chưa có tín hiệu) — phục vụ homepage_service xếp hạng top volume. Lưu kèm cache
# file dưới key "volumes" để restart không phải đợi refresh.
_volumes: dict[str, int] = {}
# Hàng đợi mã còn fetch lỗi cần retry mỗi phút (xem _drain_pending).
_pending: list[str] = []
_refresh_lock = threading.Lock()


# ===== Toán chỉ báo (port từ indicators.js) =====

def _ema(values, period):
    """EMA; out[j] ánh xạ tới values[period-1+j]. Thiếu dữ liệu → []."""
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    out = [ema]
    for i in range(period, len(values)):
        ema = values[i] * k + ema * (1 - k)
        out.append(ema)
    return out


def _macd_values(closes):
    """Mảng MACD(12,26,9); macd[m] ứng với closes[25+m] (như calcMACD trong JS)."""
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    if not ema26:
        return []
    # Tại nến 25+m: index ema12 = (25+m)-11 = 14+m; index ema26 = m
    return [ema12[m + 14] - ema26[m] for m in range(len(ema26))]


def compute_signals(candles):
    """Máy trạng thái flat↔long. candles: list dict {time, high, low, close}.

    Ánh xạ index như JS: EMA20 tại nến i = ema20[i-19]; MACD = macd[i-25];
    Signal = sig[i-33]. Vòng lặp bắt đầu i=34 (cần ≥35 nến).
    """
    closes = [c["close"] for c in candles]
    ema20 = _ema(closes, 20)
    macd = _macd_values(closes)
    sig = _ema(macd, 9)

    signals = []
    in_long = False
    for i in range(34, len(candles)):
        close = candles[i]["close"]
        ma = ema20[i - 19]
        m = macd[i - 25]
        s = sig[i - 33]
        if not in_long:
            if close > ma and m > s:
                signals.append(
                    {
                        "signal": "buy",
                        "date": str(candles[i]["time"]),
                        "price": candles[i]["low"],  # neo ở giá thấp nhất nến
                    }
                )
                in_long = True
        elif close < ma and m < s:
            signals.append(
                {
                    "signal": "sell",
                    "date": str(candles[i]["time"]),
                    "price": candles[i]["high"],  # neo ở giá cao nhất nến
                }
            )
            in_long = False
    return signals


def latest_signal(candles):
    """Tín hiệu gần nhất kèm `sessions` = số phiên từ nến tín hiệu tới nến cuối
    (0 = đổi ngay ở phiên gần nhất). None nếu chưa đủ dữ liệu / chưa có tín hiệu."""
    sigs = compute_signals(candles)
    if not sigs:
        return None
    last = sigs[-1]
    idx = next(i for i, c in enumerate(candles) if str(c["time"]) == last["date"])
    return {**last, "sessions": (len(candles) - 1) - idx}


def _to_candles(raw):
    """Chuẩn hóa bản ghi thô của data_source (giá là chuỗi) → dict số. Bỏ bản ghi hỏng."""
    out = []
    for r in raw:
        try:
            out.append(
                {
                    "time": r["time"],
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                }
            )
        except (KeyError, TypeError, ValueError):
            continue
    return out


# ===== Cache file =====

def _read_cache_file(path):
    """Đọc + validate 1 file cache. Trả dict hợp lệ, hoặc None nếu thiếu/hỏng."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError):
        return None
    if isinstance(data, dict) and isinstance(data.get("signals"), dict):
        return data
    return None


def load_cache():
    """Nạp cache runtime (CACHE_FILE); thiếu/hỏng → fallback seed commit sẵn
    (SEED_FILE) để cold start có signal ngay; cả hai hỏng → rỗng (self-heal ở
    lần refresh sau)."""
    global _cache, _volumes
    data = _read_cache_file(CACHE_FILE) or _read_cache_file(SEED_FILE)
    if data:
        _cache = {"last_refresh": data.get("last_refresh"), "signals": data["signals"]}
        _volumes = data.get("volumes") or {}
    else:
        _cache = {"last_refresh": None, "signals": {}}
        _volumes = {}
    return _cache


def _save_cache():
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            # volumes lưu cạnh _cache (key riêng) — _cache giữ shape cũ cho code khác.
            json.dump({**_cache, "volumes": _volumes}, f, ensure_ascii=False)
    except OSError as e:
        logger.warning("ghi signal cache thất bại: %s", e)


# ===== Refresh nền =====

def _refresh_one(symbol, history_fn, end):
    """Fetch + tính + lưu tín hiệu cuối của 1 mã. Trả trạng thái:
      "error" — raw is None (lỗi/rate-limit) → ỨNG VIÊN retry
      "empty" — raw == [] (rỗng thật)        → KHÔNG retry
      "ok"    — fetch được (dù có hay không có tín hiệu để lưu)
    Chỉ ghi vào _cache khi tính được tín hiệu (sig is not None); mã fetch được
    nhưng chưa có tín hiệu → "ok", không lưu (giữ nguyên hành vi cũ)."""
    raw = history_fn(symbol, HISTORY_START, end)
    if raw is None:
        return "error"
    if not raw:  # []  → rỗng thật
        return "empty"
    _record_volume(symbol, raw)  # lưu volume kể cả khi chưa có tín hiệu
    sig = latest_signal(_to_candles(raw))
    if sig is not None:
        _cache["signals"][symbol] = sig
    return "ok"


def _record_volume(symbol, raw):
    """Lưu khối lượng khớp của candle gần nhất (số nguyên). Bỏ qua nếu thiếu/hỏng."""
    try:
        _volumes[symbol] = int(float(raw[-1]["volume"]))
    except (KeyError, IndexError, TypeError, ValueError):
        pass


def refresh_signals(history_fn=None, throttle_s=THROTTLE_S, sleep_fn=time.sleep):
    """Fetch lịch sử từng mã VN100 (throttle), tính tín hiệu cuối, cache ra file.

    Vòng 1 quét toàn bộ; mã fetch lỗi (None) gom lại retry ở vòng 2 với backoff
    luỹ thừa (rate-limit hồi). Mã rỗng ([]) hoặc lỗi cả 2 vòng → GIỮ tín hiệu cũ,
    không xóa. refresh_lock chặn chạy chồng (trả cache hiện tại nếu đang refresh).
    """
    if not _refresh_lock.acquire(blocking=False):
        logger.info("refresh đang chạy — bỏ qua lần gọi chồng")
        return _cache
    try:
        history_fn = history_fn or data_source.fetch_intraday_history
        # Chỉ tính tín hiệu cho mã value > ngưỡng (cùng tập với bảng hiển thị),
        # không quét toàn nhóm VN100.
        symbols = vn100_service.get_active_symbols()
        if not symbols:
            return _cache
        end = datetime.now(VN_TZ).strftime("%Y-%m-%d")

        # Vòng 1: toàn bộ mã (pace như cũ qua throttle_s)
        failed = []
        for idx, symbol in enumerate(symbols):
            if _refresh_one(symbol, history_fn, end) == "error":
                failed.append(symbol)
            if throttle_s and idx < len(symbols) - 1:
                sleep_fn(throttle_s)

        # Vòng 2: chỉ retry mã lỗi, backoff luỹ thừa. Mã vẫn lỗi sau vòng 2 → gom
        # vào _pending để retry nền mỗi phút (xem _drain_pending).
        global _pending
        still_failed = []
        if failed:
            sleep_fn(RETRY_COOLDOWN_S)
            delay = RETRY_BASE_S
            for idx, symbol in enumerate(failed):
                outcome = _refresh_one(symbol, history_fn, end)
                if idx < len(failed) - 1:
                    sleep_fn(delay)  # nghỉ delay HIỆN TẠI trước mã kế
                if outcome == "error":
                    still_failed.append(symbol)
                    delay = min(delay * RETRY_BACKOFF_FACTOR, RETRY_MAX_BACKOFF_S)
                else:
                    delay = RETRY_BASE_S  # thành công → reset về base
        _pending = still_failed

        _cache["last_refresh"] = end
        _save_cache()
        return _cache
    finally:
        _refresh_lock.release()


def retry_pending_once(history_fn=None, throttle_s=THROTTLE_S, sleep_fn=time.sleep):
    """Một lượt retry các mã đang fetch lỗi còn sót (_pending) do rate-limit.

    Mã nào fetch được (ok/empty) → coi như xong, loại khỏi _pending (và cập nhật
    tín hiệu vào cache nếu có); mã vẫn lỗi → giữ lại cho lượt sau. Trả _pending
    còn lại ([] nghĩa là đã đủ). Dùng chung _refresh_lock để không chạy chồng với
    refresh ngày."""
    global _pending
    if not _refresh_lock.acquire(blocking=False):
        return _pending
    try:
        pending = _pending
        if not pending:
            return _pending
        history_fn = history_fn or data_source.fetch_intraday_history
        end = datetime.now(VN_TZ).strftime("%Y-%m-%d")
        remaining = []
        for idx, symbol in enumerate(pending):
            if _refresh_one(symbol, history_fn, end) == "error":
                remaining.append(symbol)
            if throttle_s and idx < len(pending) - 1:
                sleep_fn(throttle_s)
        _pending = remaining
        _save_cache()
        return _pending
    finally:
        _refresh_lock.release()


def attach_signals(board):
    """Gắn tín hiệu gần nhất vào mỗi row board theo symbol:
      signal          — "buy"/"sell"/None
      signal_date     — ngày phát tín hiệu (None nếu chưa có)
      signal_price    — giá tại điểm tín hiệu (None nếu chưa có)
      signal_sessions — số phiên từ điểm tín hiệu tới phiên gần nhất (0 = đổi hôm nay)
    Dùng key có tiền tố `signal_` để không đè `price`/giá hiện tại của board.
    `.get("sessions")` để tương thích cache/seed cũ chưa có field này (→ None)."""
    sigs = _cache["signals"]
    for row in board:
        entry = sigs.get(row.get("symbol"))
        row["signal"] = entry["signal"] if entry else None
        row["signal_date"] = entry["date"] if entry else None
        row["signal_price"] = entry["price"] if entry else None
        row["signal_sessions"] = entry.get("sessions") if entry else None
    return board


def volumes_snapshot():
    """Bản sao {symbol: volume} khối lượng phiên gần nhất — cho service khác xếp hạng."""
    return dict(_volumes)


def signal_of(symbol):
    """Xu hướng mua/bán gần nhất của 1 mã: "buy"/"sell"/None."""
    entry = _cache["signals"].get(symbol)
    return entry["signal"] if entry else None


# ===== Lập lịch =====

def _seconds_until_next_refresh(now=None):
    """Số giây tới mốc ~15:05 giờ VN kế tiếp (cuộn sang hôm sau nếu đã qua)."""
    now = now or datetime.now(VN_TZ)
    target = now.replace(
        hour=REFRESH_HOUR, minute=REFRESH_MINUTE, second=0, microsecond=0
    )
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


async def _drain_pending(sleep_s=RETRY_INTERVAL_S):
    """Sau mỗi lần refresh, cứ mỗi phút retry nhóm mã còn fetch lỗi (rate-limit)
    cho tới khi _pending rỗng — để mã chưa có tín hiệu không phải đợi lịch hôm sau."""
    while _pending:
        await asyncio.sleep(sleep_s)
        await asyncio.to_thread(retry_pending_once)


async def scheduler_loop():
    """Warm nếu cache cũ, sau đó refresh 1 lần/ngày sau đóng cửa. Sau mỗi refresh,
    vét nốt mã còn rate-limit mỗi phút (_drain_pending). Chạy qua to_thread để không
    chặn event loop (vnstock là call đồng bộ)."""
    today = datetime.now(VN_TZ).strftime("%Y-%m-%d")
    if _cache.get("last_refresh") != today:
        await asyncio.to_thread(refresh_signals)
    await _drain_pending()
    while True:
        await asyncio.sleep(_seconds_until_next_refresh())
        await asyncio.to_thread(refresh_signals)
        await _drain_pending()
