"""Test data_source offline â€” inject history_fn giáº£, khÃ´ng gá»i vnstock tháº­t."""

import datetime as dt
import logging
import time

import pandas as pd
import pytest

import data_source


class _FakeDF:
    """Giáº£ DataFrame tá»‘i thiá»ƒu Ä‘á»ƒ _map_history cháº¡y: chá»‰ cáº§n .empty + .columns."""

    empty = False
    columns = ["time", "open", "high", "low", "close"]

    def __getitem__(self, _):
        return self

    def dropna(self, **_):
        return self

    def astype(self, _):
        return self

    def to_dict(self, orient=None):
        return [{"time": "2026-06-22", "open": "1", "high": "1", "low": "1", "close": "1"}]


def test_fetch_intraday_history_passes_interval_to_history_fn():
    seen = {}

    def history_fn(symbol, start, end, interval):
        seen.update(symbol=symbol, start=start, end=end, interval=interval)
        return _FakeDF()

    data_source.fetch_intraday_history(
        "TCB", "2025-01-01", "2026-06-22", interval="1H", history_fn=history_fn
    )
    assert seen == {
        "symbol": "TCB",
        "start": "2025-01-01",
        "end": "2026-06-22",
        "interval": "1H",
    }


def test_fetch_intraday_history_defaults_interval_daily():
    """Gá»i khÃ´ng truyá»n interval â†’ máº·c Ä‘á»‹nh '1D' (giá»¯ tÆ°Æ¡ng thÃ­ch signal_service)."""
    seen = {}

    def history_fn(symbol, start, end, interval):
        seen["interval"] = interval
        return _FakeDF()

    data_source.fetch_intraday_history(
        "TCB", "2025-01-01", "2026-06-22", history_fn=history_fn
    )
    assert seen["interval"] == "1D"


def test_fetch_intraday_history_drops_nan_ohlc_rows():
    """vnstock pad náº¿n giá» nghá»‰/lá»… báº±ng NaN OHLC â†’ pháº£i bá», khÃ´ng emit 'nan'."""
    df = pd.DataFrame(
        [
            {"time": "2026-06-15", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0},
            # náº¿n rÃ¡c: giá» nghá»‰ trÆ°a, vnstock tráº£ NaN
            {"time": "2026-06-15 11:30:00", "open": float("nan"),
             "high": float("nan"), "low": float("nan"), "close": float("nan")},
            {"time": "2026-06-16", "open": 2.0, "high": 2.2, "low": 1.8, "close": 2.1},
        ]
    )
    out = data_source.fetch_intraday_history(
        "X", "a", "b", history_fn=lambda *a, **k: df
    )
    assert len(out) == 2
    assert [r["time"] for r in out] == [1781456400, 1781542800]
    # khÃ´ng cÃ²n chuá»—i "nan" trong báº¥t ká»³ giÃ¡ trá»‹ nÃ o
    assert all("nan" not in str(v).lower() for r in out for v in r.values())


def _board_df(rows):
    """Giáº£ price_board (MultiIndex) vá»›i 3 má»©c bid + 3 má»©c ask (volume + price).
    rows: list of (symbol, bid_vols[3], ask_vols[3], bid_prices[3], ask_prices[3])."""
    cols = pd.MultiIndex.from_tuples(
        [("listing", "symbol")]
        + [("bid_ask", f"bid_{i}_volume") for i in (1, 2, 3)]
        + [("bid_ask", f"ask_{i}_volume") for i in (1, 2, 3)]
        + [("bid_ask", f"bid_{i}_price") for i in (1, 2, 3)]
        + [("bid_ask", f"ask_{i}_price") for i in (1, 2, 3)]
    )
    data = [[s] + list(bv) + list(av) + list(bp) + list(ap) for s, bv, av, bp, ap in rows]
    return pd.DataFrame(data, columns=cols)


def test_fetch_market_bid_ask_sums_volumes_and_picks_max_prices():
    # bid giáº£m dáº§n (bid_1 cao nháº¥t), ask tÄƒng dáº§n (ask_3 cao nháº¥t)
    df = _board_df(
        [
            ("AAA", [10, 20, 30], [1, 2, 3], [100, 99, 98], [101, 102, 103]),
            ("BBB", [100, 0, 0], [0, 0, 0], [50, 49, 48], [51, 52, 53]),
        ]
    )
    out = data_source.fetch_market_bid_ask(["AAA", "BBB"], price_board_fn=lambda s: df)
    assert out == [
        {"symbol": "AAA", "bid_volume": 60, "ask_volume": 6, "max_bid_price": 100, "max_ask_price": 103},
        {"symbol": "BBB", "bid_volume": 100, "ask_volume": 0, "max_bid_price": 50, "max_ask_price": 53},
    ]


def test_fetch_market_bid_ask_treats_nan_as_zero():
    df = _board_df(
        [("AAA", [10, float("nan"), 5], [float("nan"), 2, 3], [100, 99, 98], [101, 102, 103])]
    )
    out = data_source.fetch_market_bid_ask(["AAA"], price_board_fn=lambda s: df)
    assert out == [
        {"symbol": "AAA", "bid_volume": 15, "ask_volume": 5, "max_bid_price": 100, "max_ask_price": 103}
    ]


def test_fetch_market_bid_ask_error_returns_none():
    def boom(s):
        raise RuntimeError("rate limit")

    assert data_source.fetch_market_bid_ask(["AAA"], price_board_fn=boom) is None


def test_fetch_market_bid_ask_empty_returns_empty():
    assert data_source.fetch_market_bid_ask([], price_board_fn=lambda s: pd.DataFrame()) == []


def test_fetch_all_symbols_returns_list():
    assert data_source.fetch_all_symbols(listing_fn=lambda: ["AAA", "BBB"]) == ["AAA", "BBB"]


def test_fetch_all_symbols_error_returns_none():
    def boom():
        raise RuntimeError("net")

    assert data_source.fetch_all_symbols(listing_fn=boom) is None


def test_fetch_industry_map_handles_string_icb_level():
    """Báº£n vnstock khÃ¡c cÃ³ thá»ƒ tráº£ icb_level kiá»ƒu CHUá»–I â†’ váº«n pháº£i lá»c Ä‘Ãºng cáº¥p 3,
    khÃ´ng Ä‘á»ƒ rá»—ng (regression production: heatmap/sectors máº¥t phÃ¢n loáº¡i)."""
    df = pd.DataFrame(
        [
            {"symbol": "AAA", "icb_level": "3", "icb_code": "8350", "icb_name": "NH"},
            {"symbol": "BBB", "icb_level": "2", "icb_code": "8000", "icb_name": "TC"},
            {"symbol": "CCC", "icb_level": "3", "icb_code": "8600", "icb_name": "BÄS"},
        ]
    )
    out = data_source.fetch_industry_map(industries_fn=lambda: df)
    assert out == {
        "AAA": {"icb_code": "8350", "icb_name": "NH"},
        "CCC": {"icb_code": "8600", "icb_name": "BÄS"},
    }


def test_fetch_industry_map_returns_none_on_bad_shape():
    """Äá»•i tÃªn cá»™t (shape láº¡) â†’ tráº£ None (Ä‘Ãºng há»£p Ä‘á»“ng 'fetch há»ng â†’ None'),
    KHÃ”NG nÃ©m lá»—i lÃ m cháº¿t market_refresher / request heatmap."""
    bad = pd.DataFrame([{"symbol": "AAA", "khong_co_icb_level": 3}])
    assert data_source.fetch_industry_map(industries_fn=lambda: bad) is None


def _match_board_df(rows):
    """Giáº£ price_board nhÃ¡nh match/listing Ä‘á»ƒ test _map_board.
    rows = list of (symbol, ref_price, match_price, accumulated_value)."""
    cols = pd.MultiIndex.from_tuples(
        [
            ("listing", "symbol"), ("listing", "ref_price"),
            ("match", "match_price"), ("match", "accumulated_value"),
            ("match", "open_price"), ("match", "highest"), ("match", "lowest"),
            ("match", "accumulated_volume"),
        ]
    )
    data = [[s, ref, p, val, p, p, p, 7000] for s, ref, p, val in rows]
    return pd.DataFrame(data, columns=cols)


def test_map_board_drops_rows_without_symbol():
    """price_board toÃ n TT cÃ³ thá»ƒ tráº£ dÃ²ng symbol = NaN (mÃ£ khÃ´ng cÃ³ listingInfo)
    â†’ pháº£i Bá»Ž: key NaN trong dict /quotes lÃ m json.dumps 500 cáº£ endpoint
    (bug phÃ¡t hiá»‡n khi verify 07/07/2026)."""
    df = _match_board_df([("AAA", 10.0, 11.0, 5.0), (float("nan"), 10.0, 10.0, 1.0)])
    out = data_source.fetch_vn100_board(["AAA"], price_board_fn=lambda s: df)
    assert [r["symbol"] for r in out] == ["AAA"]


def test_fetch_vn100_board_treats_nan_accumulated_value_as_zero():
    """MÃ£ chÆ°a khá»›p cÃ³ accumulated_value = NaN â†’ value pháº£i lÃ  0, khÃ´ng lan NaN."""
    df = _match_board_df([("AAA", 10.0, 11.0, 5.0), ("BBB", 10.0, 10.0, float("nan"))])
    out = data_source.fetch_vn100_board(["AAA", "BBB"], price_board_fn=lambda s: df)
    assert out[0]["value"] == 5_000_000
    assert out[1]["value"] == 0


def _hist_df(rows):
    """Giáº£ history 1D: rows = list of (time, volume)."""
    return pd.DataFrame(
        [
            {"time": t, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": v}
            for t, v in rows
        ]
    )


def test_fetch_prev_session_volume_picks_latest_before_today():
    """Chá»n náº¿n cÃ³ ngÃ y < hÃ´m nay gáº§n nháº¥t; bá» qua náº¿n hÃ´m nay."""
    df = _hist_df([("2026-06-24", 100), ("2026-06-25", 200), ("2026-06-26", 999)])
    out = data_source.fetch_prev_session_volume(
        "VNINDEX", history_fn=lambda *a, **k: df, today=dt.date(2026, 6, 26)
    )
    assert out == 200


def test_fetch_prev_session_volume_zero_when_no_prior_session():
    """Chá»‰ cÃ³ náº¿n hÃ´m nay (mÃ£ má»›i niÃªm yáº¿t / nghá»‰ dÃ i) â†’ 0, khÃ´ng pháº£i None."""
    df = _hist_df([("2026-06-26", 999)])
    out = data_source.fetch_prev_session_volume(
        "VNINDEX", history_fn=lambda *a, **k: df, today=dt.date(2026, 6, 26)
    )
    assert out == 0


def test_fetch_prev_session_volume_none_on_error():
    def boom(*a, **k):
        raise RuntimeError("rate limit")

    out = data_source.fetch_prev_session_volume(
        "VNINDEX", history_fn=boom, today=dt.date(2026, 6, 26)
    )
    assert out is None


def test_fetch_prev_session_volume_coerces_volume_to_int():
    """volume tá»« history lÃ  chuá»—i (astype str) â†’ pháº£i Ã©p vá» int."""
    df = _hist_df([("2026-06-25", 12345)])
    out = data_source.fetch_prev_session_volume(
        "VNINDEX", history_fn=lambda *a, **k: df, today=dt.date(2026, 6, 26)
    )
    assert out == 12345
    assert isinstance(out, int)


def test_fetch_prev_session_volume_default_today_uses_vn_tz():
    """Máº·c Ä‘á»‹nh 'hÃ´m nay' theo giá» VN, khÃ´ng theo mÃºi giá» mÃ¡y chá»§."""
    from datetime import datetime

    captured = {}

    def history(symbol, start, end, interval="1D"):
        captured["end"] = end
        return _hist_df([("2000-01-01", 1)])

    data_source.fetch_prev_session_volume("VNINDEX", history_fn=history)
    assert captured["end"] == datetime.now(data_source.VN_TZ).date().isoformat()


def _full_board_df(rows):
    """Giáº£ price_board Ä‘áº§y Ä‘á»§: cá»™t match/listing (cho _map_board) + bid_ask (cho
    _map_bid_ask), Ä‘á»ƒ test fetch_market_snapshot map Ä‘Æ°á»£c cáº£ hai tá»« 1 DataFrame.
    rows = list of (symbol, ref_price, match_price, accumulated_value)."""
    cols = pd.MultiIndex.from_tuples(
        [
            ("listing", "symbol"), ("listing", "ref_price"),
            ("match", "match_price"), ("match", "accumulated_value"),
            ("match", "open_price"), ("match", "highest"), ("match", "lowest"),
            ("match", "accumulated_volume"),
        ]
        + [("bid_ask", f"bid_{i}_volume") for i in (1, 2, 3)]
        + [("bid_ask", f"ask_{i}_volume") for i in (1, 2, 3)]
        + [("bid_ask", f"bid_{i}_price") for i in (1, 2, 3)]
        + [("bid_ask", f"ask_{i}_price") for i in (1, 2, 3)]
    )
    data = [
        [s, ref, p, val, p, p, p, 7000, 10, 20, 30, 1, 2, 3, 100, 99, 98, 101, 102, 103]
        for s, ref, p, val in rows
    ]
    return pd.DataFrame(data, columns=cols)


def test_fetch_market_snapshot_maps_board_and_bid_ask_from_one_call():
    """1 request price_board â†’ cáº£ board (match) láº«n bid/ask, khÃ´ng gá»i 2 láº§n."""
    calls = {"n": 0}

    def price_board(symbols):
        calls["n"] += 1
        return _full_board_df([("AAA", 10.0, 11.0, 5.0)])

    out = data_source.fetch_market_snapshot(["AAA"], price_board_fn=price_board)
    assert calls["n"] == 1
    assert out["board"][0]["symbol"] == "AAA"
    assert out["board"][0]["value"] == 5_000_000
    assert out["board"][0]["volume"] == 7000  # KL khá»›p tÃ­ch lÅ©y â€” cho /quotes
    assert out["bid_ask"][0] == {
        "symbol": "AAA", "bid_volume": 60, "ask_volume": 6,
        "max_bid_price": 100, "max_ask_price": 103,
    }


def test_fetch_market_snapshot_error_returns_none():
    def boom(symbols):
        raise RuntimeError("rate limit")

    assert data_source.fetch_market_snapshot(["AAA"], price_board_fn=boom) is None


def test_fetch_market_snapshot_empty_returns_empty_lists():
    out = data_source.fetch_market_snapshot([], price_board_fn=lambda s: pd.DataFrame())
    assert out == {"board": [], "bid_ask": []}


def test_fetch_intraday_history_keeps_volume_column():
    """vnstock history cÃ³ cá»™t volume â†’ pháº£i giá»¯ Ä‘á»ƒ FE váº½ biá»ƒu Ä‘á»“ Volume."""
    df = pd.DataFrame(
        [
            {"time": "2026-06-15", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "volume": 12345},
            {"time": "2026-06-16", "open": 2.0, "high": 2.2, "low": 1.8, "close": 2.1, "volume": 67890},
        ]
    )
    out = data_source.fetch_intraday_history(
        "X", "a", "b", history_fn=lambda *a, **k: df
    )
    assert out[0]["volume"] == "12345"
    assert out[1]["volume"] == "67890"


def test_fetch_vn100_members_ok():
    """listing_fn tráº£ list â†’ tráº£ nguyÃªn list."""
    out = data_source.fetch_vn100_members(listing_fn=lambda: ["ACB", "VNM"])
    assert out == ["ACB", "VNM"]


def test_fetch_vn100_members_error_returns_none():
    """listing_fn nÃ©m lá»—i (rate-limit/máº¡ng) â†’ tráº£ None, khÃ´ng nÃ©m tiáº¿p."""

    def boom():
        raise RuntimeError("rate limit")

    assert data_source.fetch_vn100_members(listing_fn=boom) is None


def _reset_exchange_memo(monkeypatch):
    monkeypatch.setattr(data_source, "_exchange_map", {})
    monkeypatch.setattr(data_source, "_exchange_map_date", None)


def test_symbol_exchange_map_builds_from_listing(monkeypatch):
    _reset_exchange_memo(monkeypatch)
    df = pd.DataFrame({"symbol": ["AAA", "BBB"], "exchange": ["HSX", "HNX"]})
    out = data_source._symbol_exchange_map(listing_fn=lambda: df)
    assert out == {"AAA": "HSX", "BBB": "HNX"}


def test_symbol_exchange_map_memoizes_same_day(monkeypatch):
    _reset_exchange_memo(monkeypatch)
    calls = {"n": 0}

    def listing_fn():
        calls["n"] += 1
        return pd.DataFrame({"symbol": ["AAA"], "exchange": ["HSX"]})

    data_source._symbol_exchange_map(listing_fn=listing_fn)
    data_source._symbol_exchange_map(listing_fn=listing_fn)
    assert calls["n"] == 1


def test_symbol_exchange_map_keeps_stale_on_error(monkeypatch):
    monkeypatch.setattr(data_source, "_exchange_map", {"AAA": "HSX"})
    monkeypatch.setattr(data_source, "_exchange_map_date", "2020-01-01")

    def boom():
        raise RuntimeError("net")

    out = data_source._symbol_exchange_map(listing_fn=boom)
    assert out == {"AAA": "HSX"}


def test_default_price_board_splits_by_exchange(monkeypatch):
    """Chia mã theo sàn, gọi board_fetch_fn riêng từng nhóm thay vì 1 request
    gộp toàn bộ."""
    _reset_exchange_memo(monkeypatch)
    listing_df = pd.DataFrame(
        {"symbol": ["AAA", "BBB", "CCC"], "exchange": ["HSX", "HNX", "UPCOM"]}
    )
    calls = []

    def board_fetch_fn(symbols):
        calls.append(sorted(symbols))
        return _match_board_df([(s, 10.0, 11.0, 1.0) for s in symbols])

    data_source._default_price_board(
        ["AAA", "BBB", "CCC"], board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
    )
    assert sorted(calls) == [["AAA"], ["BBB"], ["CCC"]]


def test_default_price_board_merges_successful_groups(monkeypatch):
    _reset_exchange_memo(monkeypatch)
    listing_df = pd.DataFrame({"symbol": ["AAA", "BBB"], "exchange": ["HSX", "HNX"]})

    def board_fetch_fn(symbols):
        return _match_board_df([(s, 10.0, 11.0, 1.0) for s in symbols])

    out = data_source._default_price_board(
        ["AAA", "BBB"], board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
    )
    assert sorted(out[("listing", "symbol")].tolist()) == ["AAA", "BBB"]


def test_default_price_board_partial_failure_keeps_successful_groups(monkeypatch):
    """1 sàn lỗi (vd UPCOM timeout) không làm fail toàn bộ — vẫn gộp dữ liệu
    các sàn thành công."""
    _reset_exchange_memo(monkeypatch)
    listing_df = pd.DataFrame({"symbol": ["AAA", "BBB"], "exchange": ["HSX", "UPCOM"]})

    def board_fetch_fn(symbols):
        if symbols == ["BBB"]:
            raise RuntimeError("timeout")
        return _match_board_df([(s, 10.0, 11.0, 1.0) for s in symbols])

    out = data_source._default_price_board(
        ["AAA", "BBB"], board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
    )
    assert out[("listing", "symbol")].tolist() == ["AAA"]


def test_default_price_board_raises_when_all_groups_fail(monkeypatch):
    _reset_exchange_memo(monkeypatch)
    listing_df = pd.DataFrame({"symbol": ["AAA"], "exchange": ["HSX"]})

    def board_fetch_fn(symbols):
        raise RuntimeError("timeout")

    with pytest.raises(RuntimeError):
        data_source._default_price_board(
            ["AAA"], board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
        )


def test_default_price_board_unknown_exchange_still_included(monkeypatch):
    """Mã không tra được sàn (không có trong bản đồ) vẫn được gửi request
    riêng, không bị rớt khỏi kết quả."""
    _reset_exchange_memo(monkeypatch)
    listing_df = pd.DataFrame({"symbol": ["AAA"], "exchange": ["HSX"]})

    def board_fetch_fn(symbols):
        return _match_board_df([(s, 10.0, 11.0, 1.0) for s in symbols])

    out = data_source._default_price_board(
        ["AAA", "ZZZ"], board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
    )
    assert sorted(out[("listing", "symbol")].tolist()) == ["AAA", "ZZZ"]


def test_default_price_board_fetches_groups_concurrently(monkeypatch):
    """3 sàn phải fetch song song, không tuần tự — tuần tự sẽ mất ~3×0.2s,
    song song chỉ mất ~0.2s (thời gian 1 request chậm nhất)."""
    _reset_exchange_memo(monkeypatch)
    listing_df = pd.DataFrame(
        {"symbol": ["AAA", "BBB", "CCC"], "exchange": ["HSX", "HNX", "UPCOM"]}
    )

    def board_fetch_fn(symbols):
        time.sleep(0.2)
        return _match_board_df([(s, 10.0, 11.0, 1.0) for s in symbols])

    start = time.monotonic()
    data_source._default_price_board(
        ["AAA", "BBB", "CCC"], board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
    )
    elapsed = time.monotonic() - start
    assert elapsed < 0.5


def test_default_price_board_logs_slow_group(monkeypatch, caplog):
    """Đo đạc hiện trường: nhóm sàn fetch lâu hơn ngưỡng SLOW_FETCH_LOG_S phải
    để lại log WARNING kèm thời gian + số mã — chẩn đoán timeout phía vendor."""
    _reset_exchange_memo(monkeypatch)
    monkeypatch.setattr(data_source, "SLOW_FETCH_LOG_S", 0.0)
    listing_df = pd.DataFrame({"symbol": ["AAA"], "exchange": ["HSX"]})

    def board_fetch_fn(symbols):
        return _match_board_df([(s, 10.0, 11.0, 1.0) for s in symbols])

    with caplog.at_level(logging.WARNING):
        data_source._default_price_board(
            ["AAA"], board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
        )
    assert "price_board sàn HSX chậm" in caplog.text


def test_default_price_board_fast_group_stays_quiet(monkeypatch, caplog):
    """Nhóm fetch nhanh (dưới ngưỡng mặc định) → KHÔNG log 'chậm' (tránh spam
    log mỗi tick trong giờ GD)."""
    _reset_exchange_memo(monkeypatch)
    listing_df = pd.DataFrame({"symbol": ["AAA"], "exchange": ["HSX"]})

    def board_fetch_fn(symbols):
        return _match_board_df([(s, 10.0, 11.0, 1.0) for s in symbols])

    with caplog.at_level(logging.WARNING):
        data_source._default_price_board(
            ["AAA"], board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
        )
    assert "chậm" not in caplog.text


def test_default_price_board_failure_log_includes_duration(monkeypatch, caplog):
    """Nhóm lỗi phải log kèm thời gian đã chờ + số mã — phân biệt fail-nhanh
    (rate-limit trả lỗi ngay) với treo-tới-timeout (~30s)."""
    _reset_exchange_memo(monkeypatch)
    listing_df = pd.DataFrame({"symbol": ["AAA", "BBB"], "exchange": ["HSX", "UPCOM"]})

    def board_fetch_fn(symbols):
        if symbols == ["BBB"]:
            raise RuntimeError("timeout")
        return _match_board_df([(s, 10.0, 11.0, 1.0) for s in symbols])

    with caplog.at_level(logging.WARNING):
        data_source._default_price_board(
            ["AAA", "BBB"], board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
        )
    assert "thất bại sau" in caplog.text
    assert "(1 mã)" in caplog.text


def test_fetch_intraday_history_logs_slow(monkeypatch, caplog):
    """history chậm hơn ngưỡng cũng phải log WARNING kèm thời gian."""
    monkeypatch.setattr(data_source, "SLOW_FETCH_LOG_S", 0.0)
    df = pd.DataFrame(
        [{"time": "2026-06-15", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0}]
    )
    with caplog.at_level(logging.WARNING):
        data_source.fetch_intraday_history(
            "SSI", "a", "b", history_fn=lambda *a, **k: df
        )
    assert "history(SSI) chậm" in caplog.text


def test_default_price_board_chunks_large_group_max_500(monkeypatch):
    """Sàn >500 mã phải chia mẻ ≤500 (khuyến nghị vendor 19/07/2026: payload
    lớn hơn làm backend VCI tính quá 30s timeout) — 1200 mã UPCOM → 3 request
    cân bằng, không mất/không trùng mã nào."""
    _reset_exchange_memo(monkeypatch)
    symbols = [f"S{i:04d}" for i in range(1200)]
    listing_df = pd.DataFrame({"symbol": symbols, "exchange": ["UPCOM"] * 1200})
    calls = []

    def board_fetch_fn(chunk):
        calls.append(list(chunk))
        return _match_board_df([(s, 10.0, 11.0, 1.0) for s in chunk])

    out = data_source._default_price_board(
        symbols, board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
    )
    assert len(calls) == 3
    assert all(len(c) <= 500 for c in calls)
    assert sorted(sym for c in calls for sym in c) == sorted(symbols)
    assert len(out) == 1200


def test_default_price_board_small_group_stays_single_request(monkeypatch):
    """Sàn ≤500 mã giữ nguyên 1 request — không chia vụn vô ích."""
    _reset_exchange_memo(monkeypatch)
    symbols = [f"S{i:04d}" for i in range(500)]
    listing_df = pd.DataFrame({"symbol": symbols, "exchange": ["HSX"] * 500})
    calls = []

    def board_fetch_fn(chunk):
        calls.append(list(chunk))
        return _match_board_df([(s, 10.0, 11.0, 1.0) for s in chunk])

    data_source._default_price_board(
        symbols, board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
    )
    assert len(calls) == 1


def test_default_price_board_chunk_failure_keeps_other_chunks(monkeypatch):
    """1 mẻ trong sàn lỗi → các mẻ còn lại (kể cả cùng sàn) vẫn được gộp."""
    _reset_exchange_memo(monkeypatch)
    symbols = [f"S{i:04d}" for i in range(600)]
    listing_df = pd.DataFrame({"symbol": symbols, "exchange": ["UPCOM"] * 600})

    def board_fetch_fn(chunk):
        if "S0000" in chunk:
            raise RuntimeError("timeout")
        return _match_board_df([(s, 10.0, 11.0, 1.0) for s in chunk])

    out = data_source._default_price_board(
        symbols, board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
    )
    assert len(out) == 300  # mẻ sau (300 mã) vẫn về đủ
