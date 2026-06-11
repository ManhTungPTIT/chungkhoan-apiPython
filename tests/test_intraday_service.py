from datetime import datetime

import intraday_service


def test_returns_data_when_fetch_ok():
    resp = intraday_service.get_intraday(
        "ACB", fetch_fn=lambda s, start, end: [{"time": "X"}]
    )
    assert resp == {"data": [{"time": "X"}]}


def test_returns_empty_when_fetch_returns_none():
    resp = intraday_service.get_intraday(
        "ACB", fetch_fn=lambda s, start, end: None
    )
    assert resp == {"data": []}


def test_passes_history_start_and_today_end_in_vn_tz():
    captured = {}

    def fetch(symbol, start, end):
        captured["symbol"] = symbol
        captured["start"] = start
        captured["end"] = end
        return []

    intraday_service.get_intraday("VNM", fetch_fn=fetch)

    assert captured["symbol"] == "VNM"
    assert captured["start"] == intraday_service.HISTORY_START
    expected_end = datetime.now(intraday_service.VN_TZ).strftime("%Y-%m-%d")
    assert captured["end"] == expected_end
