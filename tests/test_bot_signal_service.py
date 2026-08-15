"""Lớp phủ tín hiệu theo bot cho trang bộ lọc (spec 2026-08-07 §3.2–3.4).

Không gọi mạng: nến nền bơm thẳng vào signal_service._history_candles, board
bơm qua market_cache — đúng hai nguồn mà attach_signals đang dùng.
"""

import json
import os
from datetime import datetime

import pytest

from app.data import market_cache
from app.services import bot_signal_service
from app.services import signal_service

FIXTURE = os.path.join(
    os.path.dirname(__file__), "..", "..", "docs", "fixtures", "bot-signals-candles.json"
)


@pytest.fixture(scope="module")
def candles():
    with open(FIXTURE, encoding="utf-8") as f:
        return json.load(f)["candles"]


# Nến mẫu kết thúc thứ Sáu 31/07/2026 → phiên kế là thứ Hai 03/08. Chốt cứng
# `now` chứ không dùng ngày thật: để ngày trôi thì _base_stale_for_live chặn
# đường live và test đổi nghĩa theo lúc chạy.
NOW = datetime(2026, 8, 3, 10, 0, tzinfo=signal_service.VN_TZ)


def _row(symbol, close, *, open_=None, high=None, low=None):
    """Row board như price_board trả: giá VND thô (gấp PRICE_BOARD_SCALE lần nến).

    Mặc định lệch hẳn khỏi nến cuối của nền — trùng y hệt thì _phantom_candle
    coi là board đang trả lại phiên trước và chặn đường live (chống bug 04/07).
    """
    k = signal_service.PRICE_BOARD_SCALE
    open_ = close if open_ is None else open_
    return {
        "symbol": symbol,
        "price": close * k,
        "open": open_ * k,
        "high": (high if high is not None else max(open_, close) + 0.5) * k,
        "low": (low if low is not None else min(open_, close) - 0.5) * k,
        "close": close * k,
        "change_pct": 1.0,
        "value": 5_000_000_000,
    }


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, candles):
    """Mỗi test tự dựng cache sạch: hai mã cùng nền nến mẫu, cache tín hiệu ngày
    rỗng (đường live tự tính được)."""
    market_cache.reset()
    bot_signal_service.reset()
    monkeypatch.setattr(
        signal_service, "_history_candles", {"AAA": list(candles), "BBB": list(candles)}
    )
    monkeypatch.setitem(signal_service._cache, "signals", {})


def _publish_board(rows, key="market_wide"):
    market_cache.set_snapshot(key, {"vn100": {"data": rows}})


def test_trend_overlay_matches_flat_fields_of_attach_signals(candles):
    """CHỐT CHẶN CHÍNH: hai đường tính phải ra cùng một kết quả. Lệch ở đây nghĩa
    là bảng bộ lọc và panel biểu đồ sẽ nói khác nhau về cùng một mã."""
    rows = [_row("AAA", 55.0), _row("BBB", 48.0)]
    _publish_board(rows)

    expected = signal_service.attach_signals([dict(r) for r in rows], now=NOW)
    overlay = bot_signal_service.build_overlay("trend", now=NOW)["data"]

    for row in expected:
        got = overlay[row["symbol"]]
        assert got["signal"] == row["signal"]
        assert got["date"] == row["signal_date"]
        assert got["price"] == row["signal_price"]
        assert got["sessions"] == row["signal_sessions"]
        assert got["hold"] == row["signal_hold"]
        assert got["stale"] == row["signal_stale"]


def test_unknown_bot_raises_instead_of_falling_back_to_trend():
    """Im lặng rơi về Trend chính là con bug botSignals.js:16-30 đã ghi lại."""
    _publish_board([_row("AAA", 55.0)])
    with pytest.raises(KeyError):
        bot_signal_service.build_overlay("khong-co-bot-nay", now=NOW)


def test_three_bots_do_not_all_return_the_same_thing():
    """Nếu ba bot ra y hệt nhau thì việc này vô nghĩa — cũng là cách bắt lỗi
    'quên dispatch, luôn chạy compute_signals'."""
    _publish_board([_row("AAA", 55.0), _row("BBB", 48.0)])

    got = {
        bot: {s: e["signal"] for s, e in bot_signal_service.build_overlay(bot, now=NOW)["data"].items()}
        for bot in ("trend", "t", "long")
    }
    assert len({tuple(sorted(v.items())) for v in got.values()}) > 1


class TestCache:
    """Stamp bơm thẳng, KHÔNG qua set_snapshot: hai lần ghi liền nhau có thể
    trùng `fetched_at` (time.time() trên Windows phân giải ~15ms), test sẽ đỏ/xanh
    theo may rủi. Ngoài đời hai chu kỳ board cách nhau ~20s nên không dính."""

    @staticmethod
    def _pin(monkeypatch, rows, stamp):
        monkeypatch.setattr(bot_signal_service, "_board", lambda: (rows, stamp))

    def test_same_stamp_serves_cache(self, monkeypatch, candles):
        rows = [_row("AAA", 55.0)]
        self._pin(monkeypatch, rows, 1000.0)
        first = bot_signal_service.build_overlay("long", now=NOW)

        # Rút hẳn nền dưới chân: có tính lại thì kết quả PHẢI khác.
        signal_service._history_candles["AAA"] = []
        second = bot_signal_service.build_overlay("long", now=NOW)

        assert second is first

    def test_new_stamp_rebuilds(self, monkeypatch, candles):
        rows = [_row("AAA", 55.0)]
        self._pin(monkeypatch, rows, 1000.0)
        bot_signal_service.build_overlay("long", now=NOW)

        signal_service._history_candles["AAA"] = []
        self._pin(monkeypatch, rows, 2000.0)
        rebuilt = bot_signal_service.build_overlay("long", now=NOW)

        assert rebuilt["data"] == {}

    def test_each_bot_cached_separately(self, monkeypatch):
        rows = [_row("AAA", 55.0)]
        self._pin(monkeypatch, rows, 1000.0)

        t_payload = bot_signal_service.build_overlay("t", now=NOW)
        long_payload = bot_signal_service.build_overlay("long", now=NOW)

        assert t_payload["bot"] == "t"
        assert long_payload["bot"] == "long"


def test_empty_history_gives_empty_overlay_without_raising():
    signal_service._history_candles.clear()
    _publish_board([_row("AAA", 55.0)])

    assert bot_signal_service.build_overlay("t", now=NOW)["data"] == {}


class TestOpenDerived:
    def test_flag_off_when_every_candle_has_open(self):
        _publish_board([_row("AAA", 55.0)])
        entry = bot_signal_service.build_overlay("t", now=NOW)["data"]["AAA"]
        assert entry["open_derived"] is False

    def test_flag_on_when_a_candle_had_to_be_derived(self, candles):
        holed = [dict(c) for c in candles]
        del holed[80]["open"]
        signal_service._history_candles["AAA"] = holed
        _publish_board([_row("AAA", 55.0)])

        entry = bot_signal_service.build_overlay("t", now=NOW)["data"]["AAA"]
        assert entry["open_derived"] is True

    def test_symbol_dropped_when_first_candle_lacks_open(self, candles):
        holed = [dict(c) for c in candles]
        del holed[0]["open"]
        signal_service._history_candles["AAA"] = holed
        _publish_board([_row("AAA", 55.0), _row("BBB", 48.0)])

        data = bot_signal_service.build_overlay("t", now=NOW)["data"]
        assert "AAA" not in data
        assert "BBB" in data  # mã lành không bị vạ lây

    def test_long_now_reports_open_derived(self):
        """BOT Dài hạn (BOT TREND 2) ĐỌC `open` trong HAC = (O+H+L+C)/4, nên nó
        phải đi qua _with_derived_open như T+. Trước 15/08/2026 nó dùng MA50 trên
        `close` và cố ý KHÔNG suy `open` — xem spec 2026-08-15 §6."""
        _publish_board([_row("AAA", 55.0)])
        entry = bot_signal_service.build_overlay("long", now=NOW)["data"]["AAA"]
        assert "open_derived" in entry

    def test_trend_has_no_open_derived_key(self):
        _publish_board([_row("AAA", 55.0)])
        entry = bot_signal_service.build_overlay("trend", now=NOW)["data"]["AAA"]
        assert "open_derived" not in entry

    def test_prepare_candles_derives_open_for_long(self):
        candles = [
            {"time": 1, "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5},
            {"time": 2, "high": 12.0, "low": 10.0, "close": 11.0},  # thiếu open
        ]
        out, derived = signal_service.prepare_candles_for("long", candles)
        assert derived is True
        assert out[1]["open"] == 10.5  # = close của nến trước
