"""Lớp phủ tín hiệu theo BOT cho trang bộ lọc — endpoint /signals.

Xem spec docs/superpowers/specs/2026-08-07-filter-bot-signals-design.md.

Trả CHỈ phần tín hiệu, khoá theo mã: giá / % / ngành trang bộ lọc đã có sẵn từ
/vn100 nên không gửi lại. `/vn100` KHÔNG đổi — xem BOT Trend thì hệ thống chạy y
hệt trước, 0 byte và 0 CPU thêm.

Không thêm một request vnstock nào: dùng lại đúng hai nguồn attach_signals đang
dùng — nến nền trong signal_service._history_candles (RAM) và board snapshot của
market_refresher.
"""

from datetime import datetime

from app.data import market_cache

# Import trễ signal_service ở trong hàm KHÔNG cần thiết ở đây (module này không
# bị signal_service import ngược), nhưng market_cache thì phải import sớm để
# test monkeypatch được — cùng lệ với các service khác.
from app.services import signal_service

VN_TZ = signal_service.VN_TZ

# Ưu tiên snapshot toàn thị trường (dựng lại mỗi ~20s → tín hiệu cắt trong phiên
# hiện ngay), thiếu thì tới snapshot board_vn100 (~1 tiếng). Cùng thứ tự mà
# endpoint /vn100 đang dùng.
_BOARD_KEYS = ("market_wide", "board_vn100")

# {bot: {"stamp": <fetched_at>, "payload": {...}}}
_overlay_cache: dict[str, dict] = {}


def reset():
    """Xoá cache — chỉ dùng trong test để cô lập trạng thái."""
    _overlay_cache.clear()


def _board():
    """(rows, stamp) của snapshot board mới nhất. Chưa warm → ([], None)."""
    for key in _BOARD_KEYS:
        snap = market_cache.get_snapshot(key)
        rows = (snap or {}).get("vn100", {}).get("data") or []
        if rows:
            meta = market_cache.snapshot_meta(key) or {}
            return rows, meta.get("fetched_at")
    return [], None


def _entry(bot, symbol, row, today, market_base):
    """Tín hiệu của một mã theo `bot`, hoặc None khi chưa chấm được.

    Ưu tiên LIVE (ghép nến hôm nay) đúng như attach_signals. Không ghép được thì:
      - Trend  → tín hiệu đã cache ngày (_cache["signals"]) — giống hệt attach_signals
      - T+/Dài hạn → tính trên nến nền đã đóng; hai bot này KHÔNG có cache ngày
        nào, không tính ở đây thì bảng trắng trơn ngoài phiên.
    """
    base = signal_service._history_candles.get(symbol)
    merged = signal_service.live_candles(symbol, row, today)
    candles = merged if merged else base
    if not candles:
        return None, False, False

    prepared, derived = signal_service.prepare_candles_for(bot, candles)
    if prepared is None:  # T+/Dài hạn không suy được `open` (nến đầu dãy đã thiếu)
        return None, False, False

    if merged:
        entry = signal_service.latest_signal_for(bot, prepared)
        stale = False
    elif bot == "trend":
        entry = signal_service._cache["signals"].get(symbol)
        stale = bool(entry) and signal_service._base_behind_market(entry, market_base)
    else:
        entry = signal_service.latest_signal_for(bot, prepared)
        if entry is not None:
            # base_through = ngày nến CUỐI mà phép tính nhìn thấy. Thiếu nó thì
            # _base_behind_market không có gì để soi và mọi tín hiệu chết đều
            # trông như tươi.
            entry = {**entry, "base_through": signal_service._last_candle_iso(base)}
        stale = bool(entry) and signal_service._base_behind_market(entry, market_base)

    return entry, derived, stale


def build_overlay(bot, now=None):
    """Payload /signals cho một bot: {bot, generated_at, data:{symbol: {...}}}.

    Bot lạ → KeyError (api đổi thành 400). Cache theo bot, gắn với mốc fetched_at
    của snapshot board: cùng mốc → trả thẳng cache, nên bot không ai mở thì không
    bao giờ tính.
    """
    if bot not in signal_service.SIGNAL_ALGOS:
        raise KeyError(bot)

    rows, stamp = _board()
    cached = _overlay_cache.get(bot)
    if cached is not None and stamp is not None and cached["stamp"] == stamp:
        return cached["payload"]

    moment = now or datetime.now(VN_TZ)
    today = moment.date().isoformat()
    market_base = signal_service._market_base_date()

    data = {}
    for row in rows:
        symbol = row.get("symbol")
        if not symbol:
            continue
        entry, derived, stale = _entry(bot, symbol, row, today, market_base)
        if entry is None:
            continue
        item = {
            "signal": entry["signal"],
            "date": entry["date"],
            "price": entry["price"],
            "sessions": signal_service._trading_sessions_since(
                entry["date"], moment, signal_service._history_candles.get(symbol)
            ),
            "hold": entry["signal"] == "buy" and entry["date"] != today,
            "stale": stale,
        }
        # T+ và Dài hạn đều suy `open` (prepare_candles_for) — Trend thì không,
        # đừng gắn cờ chết vào nó.
        if bot in ("t", "long"):
            item["open_derived"] = derived
        data[symbol] = item

    payload = {"bot": bot, "generated_at": stamp, "data": data}
    _overlay_cache[bot] = {"stamp": stamp, "payload": payload}
    return payload
