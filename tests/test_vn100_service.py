import pytest

import vn100_service

B = 1_000_000_000


@pytest.fixture(autouse=True)
def reset_symbols():
    # Memoize dùng biến module-level → reset để test không rò rỉ trạng thái
    vn100_service._symbols = []
    yield
    vn100_service._symbols = []


def _board():
    return [
        {"symbol": "A", "price": 10, "change_pct": 1.0, "value": 8 * B},
        {"symbol": "B", "price": 20, "change_pct": 2.0, "value": 2 * B},  # < 5B → loại
        {"symbol": "C", "price": 30, "change_pct": 3.0, "value": 20 * B},
    ]


def test_process_filters_over_5b_and_sorts_value_desc():
    result = vn100_service._process(_board())
    assert [x["symbol"] for x in result] == ["C", "A"]  # B loại, sort value desc


def test_get_symbols_memoizes_after_first_fetch():
    calls = []

    def listing():
        calls.append(1)
        return ["A", "B"]

    first = vn100_service.get_symbols(fetch_fn=listing)
    second = vn100_service.get_symbols(fetch_fn=listing)
    assert first == ["A", "B"]
    assert second == ["A", "B"]
    assert len(calls) == 1  # chỉ fetch 1 lần, sau đó dùng lại RAM


def test_get_symbols_retries_when_fetch_returns_none():
    first = vn100_service.get_symbols(fetch_fn=lambda: None)
    assert first == []  # fetch hỏng → rỗng
    second = vn100_service.get_symbols(fetch_fn=lambda: ["X"])
    assert second == ["X"]  # lần sau thử lại thành công


def test_get_board_empty_when_no_symbols(monkeypatch):
    monkeypatch.setattr(vn100_service, "get_symbols", lambda: [])
    resp = vn100_service.get_board(fetch_fn=lambda s: _board())
    assert resp == {"data": []}


def test_get_board_empty_when_fetch_returns_none(monkeypatch):
    monkeypatch.setattr(vn100_service, "get_symbols", lambda: ["A"])
    resp = vn100_service.get_board(fetch_fn=lambda s: None)
    assert resp == {"data": []}


def test_get_board_processes_when_fetch_ok(monkeypatch):
    monkeypatch.setattr(vn100_service, "get_symbols", lambda: ["A", "B", "C"])
    resp = vn100_service.get_board(fetch_fn=lambda s: _board())
    assert [x["symbol"] for x in resp["data"]] == ["C", "A"]
