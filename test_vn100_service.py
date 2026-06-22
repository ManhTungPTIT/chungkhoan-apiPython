"""Test vn100_service offline — inject fetch giả, không gọi mạng."""

import vn100_service


def test_get_active_symbols_filters_by_value(monkeypatch):
    """Chỉ giữ mã value > VALUE_THRESHOLD, sắp theo value giảm dần."""
    monkeypatch.setattr(vn100_service, "_symbols", ["A", "B", "C"])
    board = [
        {"symbol": "A", "value": 10_000_000_000},
        {"symbol": "B", "value": 1_000_000_000},  # < 5 tỷ → loại
        {"symbol": "C", "value": 6_000_000_000},
    ]
    out = vn100_service.get_active_symbols(fetch_fn=lambda syms: board)
    assert out == ["A", "C"]


def test_get_active_symbols_board_error_returns_empty(monkeypatch):
    """Board fetch lỗi (None) → trả [] (self-heal lần sau)."""
    monkeypatch.setattr(vn100_service, "_symbols", ["A"])
    assert vn100_service.get_active_symbols(fetch_fn=lambda syms: None) == []
