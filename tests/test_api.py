from fastapi.testclient import TestClient

import intraday_service
import vn100_service
from api import app

# Không dùng `with TestClient` → lifespan KHÔNG chạy → không gọi vnstock thật lúc import.
client = TestClient(app)


def test_vn100_returns_data_shape(monkeypatch):
    monkeypatch.setattr(
        vn100_service,
        "get_board",
        lambda: {"data": [{"symbol": "C", "price": 30, "change_pct": 3.0, "value": 1}]},
    )
    r = client.get("/vn100")
    assert r.status_code == 200
    assert r.json() == {
        "data": [{"symbol": "C", "price": 30, "change_pct": 3.0, "value": 1}]
    }


def test_vn100_empty_on_failure_returns_200(monkeypatch):
    monkeypatch.setattr(vn100_service, "get_board", lambda: {"data": []})
    r = client.get("/vn100")
    assert r.status_code == 200
    assert r.json() == {"data": []}


def test_intraday_returns_symbol_and_data(monkeypatch):
    monkeypatch.setattr(
        intraday_service,
        "get_intraday",
        lambda symbol: {"data": [{"time": "2026-06-10", "close": "12"}]},
    )
    r = client.get("/intraday", params={"symbol": "ACB"})
    assert r.status_code == 200
    assert r.json() == {
        "symbol": "ACB",
        "data": [{"time": "2026-06-10", "close": "12"}],
    }


def test_intraday_never_returns_500_on_failure(monkeypatch):
    monkeypatch.setattr(intraday_service, "get_intraday", lambda symbol: {"data": []})
    r = client.get("/intraday", params={"symbol": "ZZZ"})
    assert r.status_code == 200  # KHÔNG 500
    assert r.json() == {"symbol": "ZZZ", "data": []}
