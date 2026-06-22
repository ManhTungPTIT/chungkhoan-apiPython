"""Test signal_service offline — inject history_fn giả, không gọi mạng, không ngủ."""

import json

import signal_service
import vn100_service


# ----- Dữ liệu nến giả -----

def _candles(closes):
    """Tạo nến từ chuỗi close; high=close+1, low=close-1, time là ngày tăng dần."""
    return [
        {"time": f"2025-{(i // 28) + 1:02d}-{(i % 28) + 1:02d}", "close": c}
        for i, c in enumerate(closes)
    ]


def _raw(closes):
    """Bản ghi 'thô' kiểu data_source (mọi giá trị là chuỗi)."""
    out = []
    for i, c in enumerate(closes):
        out.append(
            {
                "time": f"2025-{(i // 28) + 1:02d}-{(i % 28) + 1:02d}",
                "open": str(c),
                "high": str(c + 1),
                "low": str(c - 1),
                "close": str(c),
            }
        )
    return out


# ----- Toán chỉ báo -----

def test_ema_first_value_is_sma():
    vals = [1, 2, 3, 4, 5]
    ema = signal_service._ema(vals, 3)
    assert ema[0] == (1 + 2 + 3) / 3
    assert len(ema) == len(vals) - 3 + 1


def test_ema_too_short_returns_empty():
    assert signal_service._ema([1, 2], 3) == []


# ----- compute_signals / latest_signal -----

def test_fewer_than_35_candles_no_signal():
    candles = signal_service._to_candles(_raw([100 + i for i in range(34)]))
    assert signal_service.compute_signals(candles) == []
    assert signal_service.latest_signal(candles) is None


# Chuỗi giá cong: phẳng → tăng tăng tốc → giảm tăng tốc. Đường cong (không
# tuyến tính) khiến MACD vượt/thủng Signal dứt khoát (chuỗi tuyến tính làm
# MACD == Signal ở trạng thái dừng → không bao giờ kích tín hiệu).
def _curved_closes():
    flat = [100.0] * 25
    up = [100 + i * i * 0.3 for i in range(1, 31)]
    peak = up[-1]
    down = [peak - i * i * 0.5 for i in range(1, 26)]
    return flat + up + down


def test_uptrend_then_downtrend_buy_then_sell():
    candles = signal_service._to_candles(_raw(_curved_closes()))
    sigs = signal_service.compute_signals(candles)

    assert len(sigs) >= 2
    assert sigs[0]["signal"] == "buy"
    # Tín hiệu xen kẽ buy → sell → buy ...
    types = [s["signal"] for s in sigs]
    for a, b in zip(types, types[1:]):
        assert a != b
    # buy neo ở giá thấp (low = close-1); sell neo ở giá cao (high = close+1)
    buy = sigs[0]
    sell = next(s for s in sigs if s["signal"] == "sell")
    assert "date" in buy and "price" in buy
    assert isinstance(sell["price"], float)


# ----- refresh_signals -----

def _setup_cache(monkeypatch, tmp_path, symbols):
    monkeypatch.setattr(vn100_service, "_symbols", symbols)
    # refresh_signals tính cho tập mã value > ngưỡng → giả lập trả nguyên symbols.
    monkeypatch.setattr(vn100_service, "get_active_symbols", lambda: symbols)
    monkeypatch.setattr(signal_service, "_cache", {"last_refresh": None, "signals": {}})
    monkeypatch.setattr(signal_service, "CACHE_FILE", str(tmp_path / "signal_cache.json"))


def test_refresh_populates_cache_and_writes_file(monkeypatch, tmp_path):
    _setup_cache(monkeypatch, tmp_path, ["UP", "DOWN"])
    # UP: phẳng rồi tăng tăng tốc → tín hiệu cuối là buy (giữ long)
    up = _raw([100.0] * 25 + [100 + i * i * 0.3 for i in range(1, 36)])
    down = _raw([100 - 2 * i for i in range(60)])  # giảm từ đầu → không có tín hiệu

    def history(symbol, start, end):
        return up if symbol == "UP" else down

    cache = signal_service.refresh_signals(history_fn=history, throttle_s=0, sleep_fn=lambda s: None)

    assert cache["signals"]["UP"]["signal"] == "buy"
    assert cache["last_refresh"] is not None
    # Đã ghi ra file
    with open(tmp_path / "signal_cache.json", encoding="utf-8") as f:
        on_disk = json.load(f)
    assert on_disk["signals"]["UP"]["signal"] == "buy"


def test_refresh_uses_active_symbols_only(monkeypatch, tmp_path):
    """refresh chỉ tính cho mã value > ngưỡng (get_active_symbols), bỏ phần còn lại."""
    monkeypatch.setattr(signal_service, "_cache", {"last_refresh": None, "signals": {}})
    monkeypatch.setattr(signal_service, "CACHE_FILE", str(tmp_path / "c.json"))
    monkeypatch.setattr(vn100_service, "_symbols", ["BIG", "SMALL"])  # cả nhóm VN100
    monkeypatch.setattr(vn100_service, "get_active_symbols", lambda: ["BIG"])  # đã lọc
    called = []

    def history(symbol, start, end):
        called.append(symbol)
        return _buy_raw()

    signal_service.refresh_signals(history_fn=history, throttle_s=0, sleep_fn=lambda s: None)
    assert called == ["BIG"]


def test_refresh_keeps_old_signal_on_fetch_error(monkeypatch, tmp_path):
    _setup_cache(monkeypatch, tmp_path, ["TCB"])
    signal_service._cache["signals"]["TCB"] = {"signal": "buy", "date": "x", "price": 1.0}

    def history(symbol, start, end):
        return None  # lỗi rate-limit

    cache = signal_service.refresh_signals(history_fn=history, throttle_s=0, sleep_fn=lambda s: None)
    assert cache["signals"]["TCB"] == {"signal": "buy", "date": "x", "price": 1.0}


def test_refresh_keeps_old_signal_on_empty_data(monkeypatch, tmp_path):
    _setup_cache(monkeypatch, tmp_path, ["TCB"])
    signal_service._cache["signals"]["TCB"] = {"signal": "sell", "date": "y", "price": 2.0}
    calls = {"n": 0}

    def history(symbol, start, end):
        calls["n"] += 1
        return []  # rỗng thật → KHÔNG retry

    cache = signal_service.refresh_signals(
        history_fn=history, throttle_s=0, sleep_fn=lambda s: None
    )
    assert calls["n"] == 1  # rỗng không bị vớt ở vòng 2
    assert cache["signals"]["TCB"]["signal"] == "sell"


# ----- refresh_signals: retry vòng 2 + backoff -----

def _buy_raw():
    """Lịch sử cho ra tín hiệu cuối là buy (phẳng rồi tăng tăng tốc)."""
    return _raw([100.0] * 25 + [100 + i * i * 0.3 for i in range(1, 36)])


def test_refresh_retries_round1_failure(monkeypatch, tmp_path):
    """Mã lỗi (None) ở vòng 1, thành công ở vòng 2 → có tín hiệu sau refresh."""
    _setup_cache(monkeypatch, tmp_path, ["FLAKY"])
    calls = {"n": 0}

    def history(symbol, start, end):
        calls["n"] += 1
        return None if calls["n"] == 1 else _buy_raw()

    cache = signal_service.refresh_signals(
        history_fn=history, throttle_s=0, sleep_fn=lambda s: None
    )
    assert calls["n"] == 2  # vòng 1 lỗi, vòng 2 vớt lại
    assert cache["signals"]["FLAKY"]["signal"] == "buy"


def test_refresh_no_retry_when_round1_all_ok(monkeypatch, tmp_path):
    """Vòng 1 không lỗi → mỗi mã gọi đúng 1 lần, không cooldown vòng 2."""
    _setup_cache(monkeypatch, tmp_path, ["A", "B", "C"])
    calls = {}
    sleeps = []

    def history(symbol, start, end):
        calls[symbol] = calls.get(symbol, 0) + 1
        return _buy_raw()

    signal_service.refresh_signals(
        history_fn=history, throttle_s=0, sleep_fn=sleeps.append
    )
    assert calls == {"A": 1, "B": 1, "C": 1}
    assert signal_service.RETRY_COOLDOWN_S not in sleeps  # không vào vòng 2


def test_refresh_round2_backoff_grows_caps_resets(monkeypatch, tmp_path):
    """Vòng 2 lỗi liên tiếp → backoff luỹ thừa, chặn trần 30, reset 1.5 sau mã ok.

    9 mã đều lỗi vòng 1 → đều vào failed. Vòng 2 chỉ S6 thành công, còn lại lỗi.
    Delay dùng giá trị TRƯỚC khi cập nhật theo mã hiện tại, nên reset 1.5 hiện ở
    sleep của mã KẾ tiếp (S7), không phải ngay sau S6.
    """
    syms = [f"S{i}" for i in range(9)]
    _setup_cache(monkeypatch, tmp_path, syms)
    calls = {}
    sleeps = []

    def history(symbol, start, end):
        calls[symbol] = calls.get(symbol, 0) + 1
        if calls[symbol] == 1:
            return None  # vòng 1: tất cả lỗi
        return _buy_raw() if symbol == "S6" else None  # vòng 2: chỉ S6 ok

    signal_service.refresh_signals(
        history_fn=history, throttle_s=0, sleep_fn=sleeps.append
    )
    assert sleeps == [60, 1.5, 3, 6, 12, 24, 30, 30, 1.5]


# ----- retry nền mỗi phút cho mã còn rate-limit (_pending) -----

def test_refresh_sets_pending_for_persistent_failure(monkeypatch, tmp_path):
    """Mã lỗi cả vòng 1 lẫn vòng 2 → được ghi vào _pending để retry mỗi phút."""
    _setup_cache(monkeypatch, tmp_path, ["DEAD"])
    monkeypatch.setattr(signal_service, "_pending", [])

    def history(symbol, start, end):
        return None  # luôn lỗi

    signal_service.refresh_signals(history_fn=history, throttle_s=0, sleep_fn=lambda s: None)
    assert signal_service._pending == ["DEAD"]


def test_refresh_clears_pending_when_all_ok(monkeypatch, tmp_path):
    """Vòng 1 đã fetch được hết → _pending rỗng (xóa tồn dư lần trước)."""
    _setup_cache(monkeypatch, tmp_path, ["UP"])
    monkeypatch.setattr(signal_service, "_pending", ["STALE"])

    def history(symbol, start, end):
        return _buy_raw()

    signal_service.refresh_signals(history_fn=history, throttle_s=0, sleep_fn=lambda s: None)
    assert signal_service._pending == []


def test_retry_pending_resolves_and_clears(monkeypatch, tmp_path):
    """Mã trong _pending giờ fetch được → tính tín hiệu, loại khỏi _pending."""
    _setup_cache(monkeypatch, tmp_path, ["FLAKY"])
    monkeypatch.setattr(signal_service, "_pending", ["FLAKY"])

    def history(symbol, start, end):
        return _buy_raw()

    remaining = signal_service.retry_pending_once(
        history_fn=history, throttle_s=0, sleep_fn=lambda s: None
    )
    assert remaining == []
    assert signal_service._pending == []
    assert signal_service._cache["signals"]["FLAKY"]["signal"] == "buy"


def test_retry_pending_keeps_still_failing(monkeypatch, tmp_path):
    """Mã vẫn lỗi → giữ nguyên trong _pending cho lượt sau."""
    _setup_cache(monkeypatch, tmp_path, ["DEAD"])
    monkeypatch.setattr(signal_service, "_pending", ["DEAD"])

    def history(symbol, start, end):
        return None

    remaining = signal_service.retry_pending_once(
        history_fn=history, throttle_s=0, sleep_fn=lambda s: None
    )
    assert remaining == ["DEAD"]
    assert signal_service._pending == ["DEAD"]


def test_retry_pending_only_calls_pending_symbols(monkeypatch, tmp_path):
    """Chỉ fetch các mã trong _pending, không quét lại toàn rổ."""
    _setup_cache(monkeypatch, tmp_path, ["A", "B", "DEAD"])
    monkeypatch.setattr(signal_service, "_pending", ["DEAD"])
    calls = []

    def history(symbol, start, end):
        calls.append(symbol)
        return _buy_raw()

    signal_service.retry_pending_once(history_fn=history, throttle_s=0, sleep_fn=lambda s: None)
    assert calls == ["DEAD"]


def test_drain_pending_retries_each_minute_until_empty(monkeypatch):
    import asyncio

    monkeypatch.setattr(signal_service, "_pending", ["X"])
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    async def fake_to_thread(fn, *a, **k):
        return fn(*a, **k)

    calls = {"n": 0}

    def fake_retry():
        calls["n"] += 1
        signal_service._pending = [] if calls["n"] >= 2 else ["X"]
        return signal_service._pending

    monkeypatch.setattr(signal_service.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(signal_service.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(signal_service, "retry_pending_once", fake_retry)

    asyncio.run(signal_service._drain_pending(sleep_s=0))
    assert calls["n"] == 2  # lặp tới khi _pending rỗng
    assert sleeps == [0, 0]


# ----- attach_signals -----

def test_attach_signals_merges_field(monkeypatch):
    monkeypatch.setattr(
        signal_service,
        "_cache",
        {"last_refresh": "2026-06-20", "signals": {"TCB": {"signal": "buy", "date": "d", "price": 1.0}}},
    )
    board = [{"symbol": "TCB", "price": 23}, {"symbol": "VNM", "price": 50}]
    out = signal_service.attach_signals(board)
    assert out[0]["signal"] == "buy"
    assert out[1]["signal"] is None  # chưa có trong cache


# ----- load_cache -----

def test_load_cache_missing_file(monkeypatch, tmp_path):
    monkeypatch.setattr(signal_service, "CACHE_FILE", str(tmp_path / "nope.json"))
    monkeypatch.setattr(signal_service, "_cache", {"last_refresh": "x", "signals": {"A": 1}})
    cache = signal_service.load_cache()
    assert cache == {"last_refresh": None, "signals": {}}


def test_load_cache_corrupt_file(monkeypatch, tmp_path):
    f = tmp_path / "signal_cache.json"
    f.write_text("{not json", encoding="utf-8")
    monkeypatch.setattr(signal_service, "CACHE_FILE", str(f))
    monkeypatch.setattr(signal_service, "_cache", {"last_refresh": "x", "signals": {}})
    cache = signal_service.load_cache()
    assert cache == {"last_refresh": None, "signals": {}}


def test_load_cache_reads_valid_file(monkeypatch, tmp_path):
    f = tmp_path / "signal_cache.json"
    payload = {"last_refresh": "2026-06-20", "signals": {"TCB": {"signal": "buy", "date": "d", "price": 1.0}}}
    f.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(signal_service, "CACHE_FILE", str(f))
    monkeypatch.setattr(signal_service, "_cache", {"last_refresh": None, "signals": {}})
    cache = signal_service.load_cache()
    assert cache["signals"]["TCB"]["signal"] == "buy"


# ----- lịch refresh -----

def test_seconds_until_next_refresh_same_day():
    from datetime import datetime
    now = datetime(2026, 6, 20, 10, 0, 0, tzinfo=signal_service.VN_TZ)
    secs = signal_service._seconds_until_next_refresh(now)
    # 10:00 → 15:05 cùng ngày = 5h5m
    assert secs == (5 * 3600 + 5 * 60)


def test_seconds_until_next_refresh_after_close_rolls_to_tomorrow():
    from datetime import datetime
    now = datetime(2026, 6, 20, 16, 0, 0, tzinfo=signal_service.VN_TZ)
    secs = signal_service._seconds_until_next_refresh(now)
    # 16:00 → 15:05 hôm sau = 23h5m
    assert secs == (23 * 3600 + 5 * 60)
