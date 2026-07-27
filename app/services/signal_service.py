"""Tín hiệu mua/bán Trend (SMA20 + MACD) cho rổ VN100 — tính nền, cache file.

Nến *ngày* chỉ đổi tối đa 1 lần/phiên (lúc đóng cửa) nên lịch sử được fetch chậm
(throttle) 1 lần/ngày, tính tín hiệu cuối mỗi mã rồi cache ra JSON. Endpoint
/vn100 merge sẵn field `signal` từ cache → FE chỉ đọc item.signal (panel đã hỗ trợ),
không phải fetch 100 mã → né rate limit.

Logic port từ chart/src/feature/chart/untils/indicators.js (generateSignals):
vào lệnh khi close > SMA20 VÀ MACD > Signal; ra lệnh khi close < SMA20 VÀ
MACD < Signal. Đường MA20 dùng SMA (07/2026 FE đổi calcEMA → calcSMA, BE đổi
theo để chart và panel khớp tín hiệu); MACD(12,26,9) vẫn EMA đúng định nghĩa.
"""

import asyncio
import json
import logging
import math
import os
import re
import threading
import time
from datetime import date, datetime, timedelta, timezone

from app.data import data_source, market_cache
from app.services import vn100_service

logger = logging.getLogger(__name__)

VN_TZ = timezone(timedelta(hours=7))
HISTORY_START = "2025-01-01"

# Giờ giao dịch VN (T2–T6, 09:00–15:00). Các mốc refresh gốc (07:00/15:05) cố ý
# nằm NGOÀI khoảng này để fetch tín hiệu nặng (~1 mã/1.1s) không đụng vòng real-
# time 20s của market_refresher — chồng nhau sẽ vượt 60 req/phút → rate-limit.
MARKET_OPEN_HOUR = 9
MARKET_CLOSE_HOUR = 15


def _is_market_hours(now=None):
    now = now or datetime.now(VN_TZ)
    return now.weekday() < 5 and MARKET_OPEN_HOUR <= now.hour < MARKET_CLOSE_HOUR


DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
)
def _resolve_cache_file():
    """Đường dẫn cache runtime. Cho phép trỏ sang volume bền qua env
    SIGNAL_CACHE_FILE (môi trường read-only/ephemeral) — mặc định cạnh module."""
    return os.environ.get("SIGNAL_CACHE_FILE") or os.path.join(
        DATA_DIR, "signal_cache.json"
    )


CACHE_FILE = _resolve_cache_file()
# Seed commit sẵn (KHÔNG gitignore): cold start chưa có cache runtime vẫn có
# signal hiển thị ngay, khỏi đợi warm refresh. Tín hiệu nến-ngày nên seed cũ vẫn
# đúng tới 15:05; warm refresh sau đó tự cập nhật.
SEED_FILE = os.path.join(DATA_DIR, "signal_cache.seed.json")


def _resolve_history_file():
    """Đường dẫn cache nến base (_history_candles) — sống sót qua restart. Cho
    phép trỏ sang volume bền qua env SIGNAL_HISTORY_FILE, mặc định cạnh module."""
    return os.environ.get("SIGNAL_HISTORY_FILE") or os.path.join(
        DATA_DIR, "signal_history.json"
    )


# Nến base (_history_candles) trước đây CHỈ ở RAM: restart giữa phiên (deploy,
# crash, hoặc --reload lúc dev) xoá sạch → _live_signal luôn trả None →
# attach_signals đóng băng vĩnh viễn ở tín hiệu cache cũ (ảnh chụp lúc refresh
# gần nhất, có thể là nến "hôm nay" mới hình thành đầu phiên) tới tận 15:05,
# dù giá đã đổi chiều. Ghi thêm ra file (~2MB cho rổ VN100 đầy đủ lịch sử —
# rẻ hơn nhiều so với đánh đổi "luôn đóng băng tới 15:05 sau mọi lần restart")
# để nạp lại ngay lúc khởi động, khỏi đợi refresh theo lịch.
HISTORY_FILE = _resolve_history_file()
# Giãn cách giữa mỗi mã. Gói Community vnstock giới hạn 60 request/phút, nên
# throttle phải ≥1.0s (≤60/phút); 1.1s (~54/phút) để có biên an toàn — nếu nhanh
# hơn, một phần mã bị rate-limit mỗi lần refresh. ~100 mã ≈ 110s/lần.
THROTTLE_S = 1.1
# Mốc tính lại trong ngày (giờ VN): chỉ 15:05, sau khi phiên đóng cửa, cập nhật
# tín hiệu theo nến vừa hoàn tất. KHÔNG còn mốc 07:00: signal/date/price của nến
# đã đóng không đổi qua đêm, còn `signal_sessions` luôn tính ĐỘNG theo ngày hiện
# tại mỗi lần đọc (xem attach_signals) — không có gì cần "làm mới" trước giờ mở
# cửa. Gọi vnstock trong khung 7h-9h từng khiến luồng tính signal bị treo (API
# không ổn định lúc thị trường chưa mở) — xem QUIET_WINDOW/_in_quiet_window.
REFRESH_TIMES = ((15, 5),)

# Khung giờ KHÔNG được gọi vnstock cho signal, kể cả catch-up khi cache stale lúc
# khởi động lại server — né hang khi API không phản hồi tốt trước giờ mở cửa. Nếu
# 15:05 hôm trước lỗi, cache chỉ thật sự được bù lại sau 9:00 (khởi động lại sau
# khung này, hoặc tự nhiên tới mốc 15:05 hôm sau).
QUIET_WINDOW = (7, 9)  # [7:00, 9:00) giờ VN


def _in_quiet_window(now=None):
    now = now or datetime.now(VN_TZ)
    start_hour, end_hour = QUIET_WINDOW
    return start_hour <= now.hour < end_hour

# Retry vòng 2 cho mã fetch lỗi (thường do rate-limit) — backoff luỹ thừa.
RETRY_COOLDOWN_S = 60  # nghỉ 1 lần giữa vòng 1 và vòng 2 cho rate-limit hồi
RETRY_BASE_S = 1.5  # giãn cách cơ bản ở vòng 2
RETRY_BACKOFF_FACTOR = 2  # mỗi lần lỗi liên tiếp ×2
RETRY_MAX_BACKOFF_S = 30  # trần delay

# Mã fetch lỗi cả 2 vòng (rate-limit dai dẳng) được retry nền mỗi phút tới khi
# fetch được, thay vì phải đợi lịch ~15:05 hôm sau.
RETRY_INTERVAL_S = 60

# Số lần cho phép 1 mã ĐANG pending trả rỗng ([]) trước khi thôi retry. Rate-limit
# đôi khi khiến vnstock trả DataFrame RỖNG thay vì ném lỗi (→ "empty" chứ không
# phải "error"); nếu bỏ ngay ở lần rỗng đầu thì một mã có dữ liệu thật (vd DL1) bị
# loại tới tận refresh hôm sau. Cho retry vài lần: rỗng-tạm sẽ hồi, rỗng-thật
# (mã hủy niêm yết) chỉ tốn thêm ≤N fetch rồi thôi.
MAX_EMPTY_RETRIES = 3
# {symbol: số lần trả rỗng liên tiếp khi đang pending}
_empty_counts: dict[str, int] = {}

# Tỷ lệ mã active tối thiểu phải đã từng fetch (có mặt trong _volumes). Dưới mức
# này coi như rổ vừa đổi/mở rộng (nhiều mã mới chưa tính) → warm bù ngay lúc khởi
# động dù đã refresh trong ngày, thay vì đợi mốc 07:00/15:05 kế tiếp.
COVERAGE_MIN_RATIO = 0.9

# Cache nội bộ: {"last_refresh": "YYYY-MM-DD", "signals": {symbol: {signal, date, price}}}
_cache = {"last_refresh": None, "signals": {}}
# Khối lượng khớp phiên gần nhất {symbol: volume} của MỌI mã fetch được (kể cả mã
# chưa có tín hiệu) — phục vụ homepage_service xếp hạng top volume. Lưu kèm cache
# file dưới key "volumes" để restart không phải đợi refresh.
_volumes: dict[str, int] = {}
# Nến lịch sử {symbol: [candle,...]} tới phiên đã refresh (nến ngày đã đóng). Chỉ
# giữ trong RAM (không lưu file — quá lớn), phục vụ tính tín hiệu LIVE trong phiên:
# ghép nến hôm nay (giá real-time từ price_board) vào base rồi tính lại. Trống sau
# restart tới lần refresh ngày kế → attach_signals fallback về _cache["signals"].
_history_candles: dict[str, list] = {}
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


def _sma(values, period):
    """SMA trượt; out[j] ánh xạ tới values[period-1+j] (cùng quy ước _ema).
    Thiếu dữ liệu → []. Port từ smaOf của FE (indicators.js)."""
    if len(values) < period:
        return []
    total = sum(values[:period])
    out = [total / period]
    for i in range(period, len(values)):
        total += values[i] - values[i - period]
        out.append(total / period)
    return out


def _macd_values(closes):
    """Mảng MACD(12,26,9); macd[m] ứng với closes[25+m] (như calcMACD trong JS)."""
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    if not ema26:
        return []
    # Tại nến 25+m: index ema12 = (25+m)-11 = 14+m; index ema26 = m
    return [ema12[m + 14] - ema26[m] for m in range(len(ema26))]


def _candle_date(time_value):
    """Ngày ISO (giờ VN) của 1 nến. `time` trong candles LUÔN là epoch giây
    (xem data_source._map_history/_history_time_to_unix_seconds) — không phải
    chuỗi ngày sẵn. Bug 2026-07-08: compute_signals từng str() thẳng epoch,
    lưu date hỏng kiểu '1782061200' khiến _trading_sessions_since tính T+ sai
    (fromisoformat parse nhầm ngày cổ hoặc ném ValueError)."""
    return datetime.fromtimestamp(_normalize_candle_time(time_value), VN_TZ).date().isoformat()


def _normalize_candle_time(time_value):
    """Chuẩn hóa time nến về epoch giây."""
    if isinstance(time_value, (int, float)):
        if math.isnan(time_value):
            raise ValueError("invalid candle time")
        value = float(time_value)
        if value > 10_000_000_000:
            value /= 1000
        return int(value)
    if isinstance(time_value, str):
        raw = time_value.strip()
        if not raw:
            raise ValueError("empty candle time")
        if _EPOCH_DATE_RE.match(raw):
            value = int(raw)
            if value > 10_000_000_000:
                value //= 1000
            return value
        text = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=VN_TZ)
        else:
            parsed = parsed.astimezone(VN_TZ)
        return int(parsed.timestamp())
    raise ValueError("invalid candle time")


def compute_signals(candles):
    """Máy trạng thái flat↔long. candles: list dict {time, high, low, close}.

    Ánh xạ index như JS: SMA20 tại nến i = ma20[i-19]; MACD = macd[i-25];
    Signal = sig[i-33]. Vòng lặp bắt đầu i=34 (cần ≥35 nến).
    """
    closes = [c["close"] for c in candles]
    ma20 = _sma(closes, 20)
    macd = _macd_values(closes)
    sig = _ema(macd, 9)

    signals = []
    in_long = False
    for i in range(34, len(candles)):
        close = candles[i]["close"]
        ma = ma20[i - 19]
        m = macd[i - 25]
        s = sig[i - 33]
        if not in_long:
            if close > ma and m > s:
                signals.append(
                    {
                        "signal": "buy",
                        "date": _candle_date(candles[i]["time"]),
                        "price": candles[i]["low"],  # neo ở giá thấp nhất nến
                    }
                )
                in_long = True
        elif close < ma and m < s:
            signals.append(
                {
                    "signal": "sell",
                    "date": _candle_date(candles[i]["time"]),
                    "price": candles[i]["high"],  # neo ở giá cao nhất nến
                }
            )
            in_long = False
    return signals


def latest_signal(candles):
    """Tín hiệu gần nhất {signal, date, price}. None nếu chưa đủ dữ liệu / chưa có
    tín hiệu. Số phiên kể từ ngày báo (T+) KHÔNG lưu ở đây mà tính động lúc hiển
    thị trong attach_signals (xem _trading_sessions_since) để luôn đúng theo hôm nay,
    không bị đóng băng theo nến cuối lúc refresh."""
    sigs = compute_signals(candles)
    if not sigs:
        return None
    return sigs[-1]


def _weekday_sessions(start, today):
    """Số ngày thường (T2–T6) trong [start, today) — xấp xỉ số phiên khi không có
    nến lịch sử để đếm (chưa trừ nghỉ lễ). start >= today → 0."""
    if start >= today:
        return 0
    days = (today - start).days
    weeks, extra = divmod(days, 7)
    count = weeks * 5
    weekday = start.weekday()  # 0=T2 … 6=CN
    for i in range(extra):
        if (weekday + i) % 7 < 5:
            count += 1
    return count


def _trading_sessions_since(signal_date, now=None, candles=None):
    """Số phiên giao dịch đã MỞ (đạt giờ mở cửa) kể từ SAU ngày báo tới hiện tại (giờ VN).

    Ngày báo KHÔNG được tính. Một phiên chỉ được cộng khi đã tới giờ mở cửa
    (>= MARKET_OPEN_HOUR): phiên HÔM NAY trước 09:00 chưa +1, từ 09:00 mới +1 —
    nên sáng sớm (trước phiên) T+ giữ nguyên số của phiên trước, không nhảy lúc
    nửa đêm như bản cũ đếm theo lịch.

    Có `candles` (nến lịch sử phủ ngày báo, thời gian tăng dần) → đếm số nến nằm
    HẲN giữa ngày báo và hôm nay (start < d < today): nến chỉ tồn tại ở phiên
    giao dịch thật nên tự loại T7/CN LẪN ngày nghỉ lễ, không cần bảng lịch nghỉ.
    Nến chưa phủ tới sát hôm nay (cache cũ sau vài ngày service ngừng) → phần
    đuôi sau nến cuối bù bằng đếm ngày thường. Thiếu nến / nến không phủ ngày báo
    (lịch sử bị cắt ngắn) → fallback đếm ngày thường T2–T6 (chấp nhận sai số nhỏ
    quanh lễ). Cả hai nhánh cộng thêm phiên HÔM NAY nếu đã mở cửa.

    Báo hôm qua (ngày thường), xem sau 09:00 → 1, trước 09:00 → 0; báo thứ Sáu
    xem thứ Hai sau 09:00 → 1; báo hôm nay / ngày báo ở tương lai → 0. Ngày báo
    không hợp lệ / rỗng → None."""
    if not signal_date:
        return None
    try:
        start = date.fromisoformat(str(signal_date)[:10])
    except ValueError:
        return None
    now = now or datetime.now(VN_TZ)
    today = now.date()
    if start >= today:
        return 0
    # Phiên HÔM NAY chỉ tính khi đã tới giờ mở cửa (>= 09:00) và là ngày thường —
    # đây chính là chỗ "trước phiên chưa +1, sau phiên mới +1".
    opened_today = 1 if (now.weekday() < 5 and now.hour >= MARKET_OPEN_HOUR) else 0
    day_after_start = start + timedelta(days=1)
    if candles:
        try:
            dates = [date.fromisoformat(_candle_date(c["time"])) for c in candles]
        except (KeyError, TypeError, ValueError):
            dates = []
        if dates and dates[0] <= start:
            count = sum(1 for d in dates if start < d < today)
            tail = max(dates[-1] + timedelta(days=1), day_after_start)
            return count + _weekday_sessions(tail, today) + opened_today
    return _weekday_sessions(day_after_start, today) + opened_today


def _to_candles(raw):
    """Chuẩn hóa bản ghi thô của data_source (giá là chuỗi) → dict số. Bỏ bản ghi hỏng."""
    out = []
    for r in raw:
        try:
            candle = {
                "time": _normalize_candle_time(r["time"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
            }
        except (KeyError, TypeError, ValueError):
            continue
        # Volume phiên (tùy nguồn có/không) — dùng cho cột "giá trị khớp lệnh"
        # của chart Top tăng T+2 (value ≈ volume × close). Thiếu/hỏng → bỏ key,
        # không loại nến (các luồng khác chỉ cần OHLC).
        try:
            candle["volume"] = int(float(r["volume"]))
        except (KeyError, TypeError, ValueError):
            pass
        out.append(candle)
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


_EPOCH_DATE_RE = re.compile(r"^\d+$")


def _repair_legacy_epoch_dates(signals):
    """Sửa TẠI CHỖ các entry cache ghi trước bug 2026-07-08 (xem _candle_date):
    `date` bị lưu nhầm dạng epoch giây str() thẳng (vd '1782061200'), toàn chữ
    số — ISO thật luôn có '-' nên nhận diện an toàn. Mã có epoch không hợp lệ
    (hiếm) → bỏ qua, giữ nguyên."""
    for entry in signals.values():
        raw_date = entry.get("date") if isinstance(entry, dict) else None
        if isinstance(raw_date, str) and _EPOCH_DATE_RE.match(raw_date):
            try:
                entry["date"] = _candle_date(int(raw_date))
            except (OverflowError, OSError, ValueError):
                pass


def load_cache():
    """Nạp cache runtime (CACHE_FILE); thiếu/hỏng → fallback seed commit sẵn
    (SEED_FILE) để cold start có signal ngay; cả hai hỏng → rỗng (self-heal ở
    lần refresh sau). Tự sửa các date epoch hỏng còn sót từ trước fix (xem
    _repair_legacy_epoch_dates) — không đợi mã đó có tín hiệu mới mới tự lành."""
    global _cache, _volumes
    data = _read_cache_file(CACHE_FILE) or _read_cache_file(SEED_FILE)
    if data:
        _repair_legacy_epoch_dates(data["signals"])
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


def load_history_cache():
    """Nạp _history_candles (nến base cho tín hiệu live) từ HISTORY_FILE lúc
    khởi động. Thiếu/hỏng file → rỗng (self-heal ở lần refresh_signals kế —
    live-merge tạm ngưng, dùng tín hiệu cache, giống hành vi trước khi có fix
    này); KHÔNG coi là lỗi nghiêm trọng."""
    global _history_candles
    try:
        with open(HISTORY_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError):
        data = None
    _history_candles = {}
    if isinstance(data, dict):
        for symbol, candles in data.items():
            if isinstance(candles, list):
                normalized = _to_candles(candles)
                if normalized:
                    _history_candles[symbol] = normalized
    return _history_candles


def _save_history_cache():
    """Ghi _history_candles ra HISTORY_FILE — để restart giữa phiên (deploy/
    crash/--reload) vẫn nạp lại được nến base, tránh đóng băng tín hiệu tới
    15:05 (xem comment ở HISTORY_FILE)."""
    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(_history_candles, f, ensure_ascii=False)
    except OSError as e:
        logger.warning("ghi signal history cache thất bại: %s", e)


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
    candles = _to_candles(raw)
    _history_candles[symbol] = candles  # base cho tính live trong phiên
    sig = latest_signal(candles)
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
        # không quét toàn nhóm VN100. Lúc thị trường đóng (vd lịch 07:00 hoặc boot
        # trước giờ mở cửa) mọi mã có value phiên hôm nay = 0 → active rỗng; fallback
        # nguyên nhóm VN100 để vẫn tính được trên nến ngày đã đóng (không thành no-op).
        symbols = vn100_service.get_active_symbols() or vn100_service.get_symbols()
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
        _save_history_cache()
        return _cache
    finally:
        _refresh_lock.release()


def retry_pending_once(history_fn=None, throttle_s=THROTTLE_S, sleep_fn=time.sleep):
    """Một lượt retry các mã đang fetch lỗi còn sót (_pending) do rate-limit.

    Mã fetch được (ok) → loại khỏi _pending (cập nhật tín hiệu nếu có); mã lỗi
    (error) → giữ lại lượt sau; mã trả rỗng (empty) → giữ lại có giới hạn
    (MAX_EMPTY_RETRIES) vì rỗng có thể do rate-limit tạm, quá ngưỡng mới bỏ. Trả
    _pending còn lại ([] nghĩa là đã đủ). Dùng chung _refresh_lock để không chạy
    chồng với refresh ngày."""
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
            outcome = _refresh_one(symbol, history_fn, end)
            if outcome == "error":
                remaining.append(symbol)
            elif outcome == "empty":
                _empty_counts[symbol] = _empty_counts.get(symbol, 0) + 1
                if _empty_counts[symbol] < MAX_EMPTY_RETRIES:
                    remaining.append(symbol)  # rỗng-tạm (rate-limit) → thử lại
                else:
                    _empty_counts.pop(symbol, None)  # rỗng-thật → thôi, dọn đếm
            else:  # ok
                _empty_counts.pop(symbol, None)
            if throttle_s and idx < len(pending) - 1:
                sleep_fn(throttle_s)
        _pending = remaining
        _save_cache()
        _save_history_cache()
        return _pending
    finally:
        _refresh_lock.release()


# price_board (row board) trả giá VND thô (vd 62900), còn nến lịch sử (history)
# theo NGHÌN đồng (vd 62.9) → phải chia để đưa OHLC board về cùng đơn vị nến base
# trước khi ghép. Bỏ bước này → nến hôm nay vọt 1000× → close >> SMA20 → BÁO MUA
# GIẢ hàng loạt. (FE cũng /1000 khi hiển thị giá — xem filterStock.jsx.)
PRICE_BOARD_SCALE = 1000


def _live_candle(row, today):
    """Nến hôm nay từ 1 row board (giá real-time), quy về đơn vị nghìn đồng như nến
    lịch sử. `time` phải là epoch giây như MỌI nến khác trong hệ thống (xem
    _candle_date) — không phải chuỗi ngày, để so sánh/format nhất quán với base
    candles (_merge_today, _phantom_candle, compute_signals). None nếu thiếu
    OHLC/ngày hỏng (vd row test tối giản chỉ có price) → khỏi ghép, dùng tín
    hiệu cache."""
    try:
        close = float(row["close"]) / PRICE_BOARD_SCALE
        high = float(row["high"]) / PRICE_BOARD_SCALE
        low = float(row["low"]) / PRICE_BOARD_SCALE
    except (KeyError, TypeError, ValueError):
        return None
    # close<=0 = CHƯA khớp lệnh (match_price NaN → _map_board ép về 0: ATO đầu
    # phiên hoặc mã thanh khoản thấp chưa giao dịch), KHÔNG phải giá thật. Ghép
    # nến close=0 vào base sẽ kéo SMA20 sập → SELL giả tại T+0 (price 0). Coi như
    # chưa có dữ liệu live → caller (_live_signal) trả None → attach_signals giữ
    # tín hiệu cache phiên trước (đúng theo nến ngày đã đóng).
    if close <= 0:
        return None
    return {
        "time": int(datetime.strptime(today, "%Y-%m-%d").replace(tzinfo=VN_TZ).timestamp()),
        "high": high,
        "low": low,
        "close": close,
    }


def _merge_today(base, live):
    """Ghép nến hôm nay vào base: cùng ngày với nến cuối → thay thế (nến đang cập
    nhật); ngày mới → nối thêm. Không sửa base gốc. Cả 2 phía đều epoch giây
    (xem _live_candle) nên so ngày qua _candle_date, không string-slice thô."""
    if base and _candle_date(base[-1]["time"]) == _candle_date(live["time"]):
        return base[:-1] + [live]
    return base + [live]


def _last_candle_date(candles):
    if not candles:
        return None
    try:
        return date.fromisoformat(_candle_date(candles[-1]["time"]))
    except (KeyError, TypeError, ValueError):
        return None


def _previous_weekday(day):
    prev = day - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    return prev


def _cached_closed_daily_candles(symbol, today):
    payload = market_cache.peek_intraday(symbol, "1d")
    rows = payload.get("data") if isinstance(payload, dict) else payload
    candles = _to_candles(rows or [])
    if not candles:
        return []
    today_date = date.fromisoformat(today)
    return [
        candle
        for candle in candles
        if date.fromisoformat(_candle_date(candle["time"])) < today_date
    ]


def _refresh_base_from_intraday_cache(symbol, base, today):
    """Nếu chart đã tải /intraday 1d mới hơn base signal, dùng nến đã đóng đó.

    /intraday và FE dùng cùng công thức nến ngày. Đọc cache RAM, không fetch mới,
    để tránh attach_signals(/vn100) tạo thêm hàng trăm request history.
    """
    cached = _cached_closed_daily_candles(symbol, today)
    if not cached:
        return base
    cached_last = _last_candle_date(cached)
    base_last = _last_candle_date(base)
    if cached_last and (base_last is None or cached_last > base_last):
        _history_candles[symbol] = cached
        return cached
    return base


def _base_stale_for_live(base, today):
    """Không tính live trên base thiếu phiên đã đóng gần nhất.

    Ví dụ thứ Hai 27/07 mà base mới tới 23/07 thì ghép live 27/07 sẽ báo BUY
    sai thành T+0, trong khi nến 24/07 đã là điểm BUY đúng theo FE.
    """
    last = _last_candle_date(base)
    if last is None:
        return True
    return last < _previous_weekday(date.fromisoformat(today))

def _phantom_candle(last, live, today):
    """Nến live sắp bị NỐI như phiên mới trong khi hôm nay thật ra KHÔNG có dữ
    liệu mới → nến ma (bản sao phiên trước) làm SMA20/MACD lệch → tín hiệu giả.
    (Bug 04/07/2026: sáng thứ Bảy price_board trả nguyên OHLC chốt thứ Sáu, bị
    nối thành nến '2026-07-04' → hàng loạt mã cache SELL hiển thị BUY giả.)

    Chặn khi nến live sẽ được nối (khác ngày nến cuối base) VÀ:
      - hôm nay là thứ Bảy/Chủ nhật — không thể có phiên mới, hoặc
      - OHLC live trùng hệt nến cuối — board đang trả dữ liệu phiên trước
        (sáng sớm chưa mở cửa, ngày nghỉ lễ giữa tuần).
    Nghỉ lễ giữa tuần mà giá board lệch nhẹ so với nến lịch sử (điều chỉnh cổ
    tức...) vẫn lọt qua kiểm tra trùng — hiếm, chấp nhận sai số này."""
    if _candle_date(last["time"]) == today:
        return False  # cùng ngày → _merge_today thay thế nến cuối, không nối
    if date.fromisoformat(today).weekday() >= 5:
        return True
    return all(
        math.isclose(live[k], float(last[k]), rel_tol=1e-9)
        for k in ("high", "low", "close")
    )


def _live_signal(symbol, row, today):
    """Tín hiệu tính LIVE: ghép nến hôm nay của `row` vào base candles rồi tính lại.

    Thiếu base (chưa refresh/khởi động lại), row thiếu OHLC, hoặc nến live là
    nến ma (xem _phantom_candle) → None để caller fallback về tín hiệu đã cache
    (_cache["signals"])."""
    base = _history_candles.get(symbol)
    live = _live_candle(row, today)
    if not base or live is None:
        return None
    base = _refresh_base_from_intraday_cache(symbol, base, today)
    if _base_stale_for_live(base, today):
        return None
    if _phantom_candle(base[-1], live, today):
        return None
    return latest_signal(_merge_today(base, live))


def attach_signals(board, now=None):
    """Gắn tín hiệu gần nhất vào mỗi row board theo symbol:
      signal          — "buy"/"sell"/None
      signal_date     — ngày phát tín hiệu (None nếu chưa có)
      signal_price    — giá tại điểm tín hiệu (None nếu chưa có)
      signal_sessions — số phiên đã MỞ (đạt 09:00) từ SAU ngày báo tới HIỆN TẠI
                        (0 = báo hôm nay HOẶC sáng hôm sau trước giờ mở, None nếu chưa
                        có tín hiệu) — tính động theo _trading_sessions_since để luôn
                        đúng theo giờ hiện tại; đếm theo nến base nên tự loại T7/CN và lễ.
      signal_hold     — True khi tín hiệu là buy nhưng đã qua ngày báo (pha "nắm giữ").
                        Cần riêng vì signal_sessions=0 cho CẢ ngày báo lẫn sáng hôm sau
                        trước giờ mở → không suy được BUY/HOLD từ số phiên.

    Ưu tiên tín hiệu LIVE: ghép giá hôm nay (OHLC real-time trong row) vào base nến
    lịch sử rồi tính lại → tín hiệu cắt trong phiên hiện ngay, không phải đợi 15:05.
    Không tính được live (thiếu base/OHLC) → fallback tín hiệu cache _cache["signals"].
    Dùng key có tiền tố `signal_` để không đè `price`/giá hiện tại của board. `now`
    cho test bơm ngày cố định; mặc định lấy ngày hiện tại (giờ VN)."""
    today = (now or datetime.now(VN_TZ)).date().isoformat()
    sigs = _cache["signals"]
    for row in board:
        symbol = row.get("symbol")
        # Ưu tiên tín hiệu LIVE (ghép giá hôm nay vào base nến) để tín hiệu cắt
        # TRONG PHIÊN hiện ngay, không đợi 15:05. Nến hôm nay còn hình thành nên
        # tín hiệu có thể lật lại nếu giá đảo chiều trước giờ đóng cửa (whipsaw) —
        # chấp nhận đánh đổi này để panel phản ánh diễn biến thực trong phiên.
        entry = _live_signal(symbol, row, today) or sigs.get(symbol)
        row["signal"] = entry["signal"] if entry else None
        row["signal_date"] = entry["date"] if entry else None
        row["signal_price"] = entry["price"] if entry else None
        row["signal_sessions"] = (
            _trading_sessions_since(entry["date"], now, _history_candles.get(symbol))
            if entry
            else None
        )
        # Pha "nắm giữ": đã báo MUA nhưng đã qua ngày báo (hôm nay khác ngày báo).
        # Tách khỏi signal_sessions vì phiên hôm nay trước 09:00 có sessions=0
        # GIỐNG ngày báo — FE không thể phân biệt BUY/HOLD từ số phiên nữa.
        row["signal_hold"] = bool(
            entry and entry["signal"] == "buy" and entry["date"] != today
        )
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
    """Số giây tới mốc refresh GẦN NHẤT sắp tới trong REFRESH_TIMES (giờ VN); mốc
    nào đã qua trong hôm nay thì cuộn sang hôm sau."""
    now = now or datetime.now(VN_TZ)
    targets = []
    for hour, minute in REFRESH_TIMES:
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        targets.append(target)
    return (min(targets) - now).total_seconds()


def _history_gap_symbols():
    """Mã có tín hiệu cache nhưng THIẾU nến base (_history_candles) — fetch lỗi/
    rỗng ở những lần refresh trước (rate-limit dai dẳng) khiến base không được
    ghi, trong khi tín hiệu cũ vẫn giữ theo thiết kế. Không có base → _live_signal
    trả None → attach_signals kẹt ở tín hiệu cache cũ suốt phiên. (Bug HPX
    06/07/2026: chart cắt BUY trong ngày nhưng panel vẫn SELL cache từ 24/04.)

    Không đo phủ bằng _volumes như _coverage_gap: _volumes lưu vĩnh viễn từ các
    lần fetch cũ nên mã đã mất base vẫn "trông như đã phủ"."""
    return [s for s in _cache["signals"] if s not in _history_candles]


def _seed_history_gap_pending():
    """Nối mã gap (có tín hiệu, thiếu base) vào _pending — được _drain_pending
    vét mỗi phút như mã rate-limit thường: fetch được là có base, live signal
    sống lại ngay, không phải đợi refresh 15:05 hôm sau. Bỏ qua mã đã chờ sẵn."""
    _pending.extend(s for s in _history_gap_symbols() if s not in _pending)


async def _drain_pending(sleep_s=RETRY_INTERVAL_S):
    """Sau mỗi lần refresh, cứ mỗi phút retry nhóm mã còn fetch lỗi (rate-limit)
    cho tới khi _pending rỗng — để mã chưa có tín hiệu không phải đợi lịch hôm sau.

    Trước khi vét, seed thêm mã có tín hiệu nhưng thiếu nến base (xem
    _history_gap_symbols) VÀ mã lẻ mới lọt active trong phiên, chưa từng fetch
    (xem _newly_active_symbols — vd tăng trần đột biến, _coverage_gap không
    bắt được vì chỉ vài mã lẻ giữa 1 rổ đã covered) để tự vá lỗ hổng "mù live".
    Nhóm này thường chỉ vài mã lỗi sót/mới nổi nên mỗi lượt vét chỉ vài chục
    giây ở throttle 1.1s — không đáng kể so với budget rate-limit, chấp nhận
    chạy cả trong phiên (chính trong phiên mới cần live signal). KHÔNG seed
    trong QUIET_WINDOW (7h-9h): giữ nguyên tắc không gọi vnstock trước giờ mở
    cửa; khởi động trong khung này thì gap được vá ở lượt drain sau refresh
    15:05."""
    if not _in_quiet_window():
        _seed_history_gap_pending()
        _seed_newly_active_pending()
    while _pending:
        await asyncio.sleep(sleep_s)
        await asyncio.to_thread(retry_pending_once)


def _coverage_gap(active_fn=None):
    """True nếu rổ active có tỷ lệ lớn mã CHƯA từng fetch (vắng trong _volumes) —
    dấu hiệu rổ vừa đổi/mở rộng, cache cũ chưa phủ mã mới.

    Dùng _volumes (chứ không phải _cache["signals"]) làm mốc phủ: _volumes lưu MỌI
    mã đã fetch được, kể cả mã tính ra chưa có tín hiệu — nên không báo "gap" giả
    cho mã đã tính nhưng chưa có tín hiệu. Active rỗng (ngoài phiên/fetch lỗi) →
    False: không ép warm, mốc lịch sẽ lo (tránh fetch lặp khi restart ngoài giờ)."""
    active_fn = active_fn or vn100_service.get_active_symbols
    active = active_fn()
    if not active:
        return False
    covered = sum(1 for s in active if s in _volumes)
    return covered < len(active) * COVERAGE_MIN_RATIO


def _newly_active_symbols(active_fn=None):
    """Mã đang active (value > ngưỡng) nhưng CHƯA TỪNG fetch (vắng trong
    _volumes) — mã lẻ mới lần đầu lọt ngưỡng TRONG PHIÊN (vd tăng trần đột
    biến từ thanh khoản thấp). _coverage_gap không bắt được case này vì chỉ
    kích hoạt khi TỶ LỆ LỚN rổ chưa phủ; case này thường chỉ vài mã lẻ giữa
    1 rổ đã covered đầy đủ nên tỷ lệ tổng thể không tụt dưới COVERAGE_MIN_RATIO.
    Không có gì để vá nếu active rỗng (ngoài phiên/fetch lỗi)."""
    active_fn = active_fn or vn100_service.get_active_symbols
    return [s for s in active_fn() if s not in _volumes]


def _seed_newly_active_pending(active_fn=None):
    """Nối mã mới active (xem _newly_active_symbols) vào _pending — được
    _drain_pending vét mỗi phút như mã rate-limit thường, có tín hiệu ngay
    trong phiên thay vì đợi refresh 15:05 hôm sau. Bỏ qua mã đã chờ sẵn."""
    _pending.extend(s for s in _newly_active_symbols(active_fn) if s not in _pending)


async def scheduler_loop():
    """Warm khi cache cũ (khác hôm nay) HOẶC rổ active có nhiều mã chưa phủ
    (_coverage_gap — vd vừa đổi/mở rộng rổ), sau đó refresh theo REFRESH_TIMES (chỉ
    còn 15:05). Sau mỗi refresh vét nốt mã còn rate-limit mỗi phút (_drain_pending).
    Chạy qua to_thread để không chặn event loop (vnstock là call đồng bộ).

    Warm lúc khởi động BỊ CHẶN trong QUIET_WINDOW (7h-9h, xem _in_quiet_window):
    server restart trong khung này (cache "stale" vì last_refresh là hôm qua) sẽ
    KHÔNG gọi vnstock — né hang do API không ổn định trước giờ mở cửa. Tín hiệu
    dùng tạm cache/seed cũ tới khi khởi động lại ngoài khung này hoặc tới 15:05.

    KHÔNG ép refresh chỉ vì `_history_candles` (base nến live) rỗng: base chỉ ở RAM
    nên sau restart giữa phiên nó rỗng, nhưng ép refresh mỗi lần khởi động = fetch
    lại ~100 mã → dễ đụng rate-limit khi restart lặp. Chấp nhận: sau restart giữa
    phiên, tín hiệu live tạm ngưng, fallback về tín hiệu cache (vẫn đúng theo nến
    ngày) tới lượt refresh kế (15:05).

    Warm-on-gap KHÔNG gây fetch lặp khi restart: sau 1 lần warm, _volumes (lưu ra
    file) đã phủ rổ → lần khởi động sau không còn gap. Chỉ warm-on-gap NGOÀI giờ
    giao dịch: giữa phiên để mốc 15:05 lo, tránh chồng vòng real-time của
    market_refresher (cùng nã vnstock → rate-limit, nghẽn server)."""
    today = datetime.now(VN_TZ).strftime("%Y-%m-%d")
    stale = _cache.get("last_refresh") != today
    if not _in_quiet_window() and (stale or (_coverage_gap() and not _is_market_hours())):
        await asyncio.to_thread(refresh_signals)
    await _drain_pending()
    while True:
        await asyncio.sleep(_seconds_until_next_refresh())
        await asyncio.to_thread(refresh_signals)
        await _drain_pending()
