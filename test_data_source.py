"""Test data_source offline — inject history_fn giả, không gọi vnstock thật."""

import pandas as pd

import data_source


class _FakeDF:
    """Giả DataFrame tối thiểu để _map_history chạy: chỉ cần .empty + .columns."""

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
    """Gọi không truyền interval → mặc định '1D' (giữ tương thích signal_service)."""
    seen = {}

    def history_fn(symbol, start, end, interval):
        seen["interval"] = interval
        return _FakeDF()

    data_source.fetch_intraday_history(
        "TCB", "2025-01-01", "2026-06-22", history_fn=history_fn
    )
    assert seen["interval"] == "1D"


def test_fetch_intraday_history_drops_nan_ohlc_rows():
    """vnstock pad nến giờ nghỉ/lễ bằng NaN OHLC → phải bỏ, không emit 'nan'."""
    df = pd.DataFrame(
        [
            {"time": "2026-06-15", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0},
            # nến rác: giờ nghỉ trưa, vnstock trả NaN
            {"time": "2026-06-15 11:30:00", "open": float("nan"),
             "high": float("nan"), "low": float("nan"), "close": float("nan")},
            {"time": "2026-06-16", "open": 2.0, "high": 2.2, "low": 1.8, "close": 2.1},
        ]
    )
    out = data_source.fetch_intraday_history(
        "X", "a", "b", history_fn=lambda *a, **k: df
    )
    assert len(out) == 2
    assert [r["time"] for r in out] == ["2026-06-15", "2026-06-16"]
    # không còn chuỗi "nan" trong bất kỳ giá trị nào
    assert all("nan" not in v.lower() for r in out for v in r.values())


def test_fetch_intraday_history_keeps_volume_column():
    """vnstock history có cột volume → phải giữ để FE vẽ biểu đồ Volume."""
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
