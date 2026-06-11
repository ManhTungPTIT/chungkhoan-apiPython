import pandas as pd
import pytest

import data_source


def _fake_board_df():
    """Mô phỏng DataFrame MultiIndex mà Trading.price_board trả về (chỉ cột cần)."""
    cols = pd.MultiIndex.from_tuples(
        [
            ("listing", "symbol"),
            ("listing", "ref_price"),
            ("match", "match_price"),
            ("match", "accumulated_value"),
        ]
    )
    # accumulated_value của price_board tính bằng TRIỆU VND
    rows = [
        ["ACB", 100.0, 110.0, 8_000],  # 8_000 triệu = 8 tỷ VND
        ["VNM", 200.0, 190.0, 3_000],  # 3 tỷ VND
    ]
    return pd.DataFrame(rows, columns=cols)


# --- Bug gốc: SystemExit (rate-limit) không được phép thoát ra ---


def test_fetch_vn100_board_returns_none_when_source_raises_systemexit():
    def boom(symbols):
        raise SystemExit("Rate limit exceeded")

    # Không được ném SystemExit ra ngoài; phải trả None.
    assert data_source.fetch_vn100_board(["ACB"], price_board_fn=boom) is None


def test_fetch_vn100_board_returns_none_on_generic_exception():
    def boom(symbols):
        raise RuntimeError("network down")

    assert data_source.fetch_vn100_board(["ACB"], price_board_fn=boom) is None


def test_fetch_intraday_returns_none_when_source_raises_systemexit():
    def boom(symbol, start, end):
        raise SystemExit("Rate limit exceeded")

    assert (
        data_source.fetch_intraday_history(
            "ACB", "2025-01-01", "2026-06-09", history_fn=boom
        )
        is None
    )


# --- Danh sách mã VN100 ---


def test_fetch_vn100_symbols_returns_list():
    result = data_source.fetch_vn100_symbols(
        listing_fn=lambda: ["ACB", "VNM", "HPG"]
    )
    assert result == ["ACB", "VNM", "HPG"]


def test_fetch_vn100_symbols_returns_none_on_systemexit():
    def boom():
        raise SystemExit("Rate limit exceeded")

    assert data_source.fetch_vn100_symbols(listing_fn=boom) is None


# --- Map dữ liệu ---


def test_fetch_vn100_board_maps_fields_and_computes_change_pct():
    result = data_source.fetch_vn100_board(
        ["ACB", "VNM"], price_board_fn=lambda symbols: _fake_board_df()
    )
    assert result == [
        {"symbol": "ACB", "price": 110.0, "change_pct": 10.0, "value": 8_000_000_000},
        {"symbol": "VNM", "price": 190.0, "change_pct": -5.0, "value": 3_000_000_000},
    ]  # value đã quy đổi từ triệu sang VND


def test_fetch_vn100_board_handles_zero_ref_price():
    cols = pd.MultiIndex.from_tuples(
        [
            ("listing", "symbol"),
            ("listing", "ref_price"),
            ("match", "match_price"),
            ("match", "accumulated_value"),
        ]
    )
    df = pd.DataFrame([["X", 0.0, 50.0, 1_000]], columns=cols)  # 1_000 triệu
    result = data_source.fetch_vn100_board(["X"], price_board_fn=lambda s: df)
    assert result == [
        {"symbol": "X", "price": 50.0, "change_pct": 0.0, "value": 1_000_000_000}
    ]


def test_fetch_intraday_maps_ohlc_records():
    df = pd.DataFrame(
        {
            "time": ["2026-06-08", "2026-06-09"],
            "open": [10, 11],
            "high": [12, 13],
            "low": [9, 10],
            "close": [11, 12],
        }
    )
    result = data_source.fetch_intraday_history(
        "ACB", "2025-01-01", "2026-06-09", history_fn=lambda s, start, end: df
    )
    assert result == [
        {"time": "2026-06-08", "open": "10", "high": "12", "low": "9", "close": "11"},
        {"time": "2026-06-09", "open": "11", "high": "13", "low": "10", "close": "12"},
    ]
