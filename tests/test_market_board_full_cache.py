"""market_board_full persist/load — để flow_surge vẫn có universe rộng khi boot
ngoài giờ giao dịch / sáng hôm sau (RAM last-good mất sau restart)."""

from app.data import market_cache
from app.data import market_refresher


def test_save_then_load_sets_snapshot(tmp_path, monkeypatch):
    path = tmp_path / "market_board_full_cache.json"
    monkeypatch.setattr(market_refresher, "MARKET_BOARD_FULL_CACHE_FILE", str(path))
    rows = [{"symbol": "AAA", "price": 20000, "value": 6_000_000_000}]

    market_refresher._save_market_board_full_cache(rows)
    market_cache.reset()
    loaded = market_refresher.load_market_board_full_cache()

    assert loaded == rows
    assert market_cache.get_snapshot("market_board_full") == rows
    market_cache.reset()


def test_load_missing_file_returns_none(tmp_path, monkeypatch):
    path = tmp_path / "khong-ton-tai.json"
    monkeypatch.setattr(market_refresher, "MARKET_BOARD_FULL_CACHE_FILE", str(path))
    market_cache.reset()

    assert market_refresher.load_market_board_full_cache() is None
    assert market_cache.get_snapshot("market_board_full") is None
