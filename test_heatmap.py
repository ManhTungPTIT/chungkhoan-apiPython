"""Test get_heatmap offline — inject board_fn/industry_fn giả, không gọi mạng."""

import sector_service
import vn100_service


def _fake_board(symbols):
    return [
        {"symbol": "VIC", "change_pct": 6.15, "value": 480, "price": 100},
        {"symbol": "VHM", "change_pct": 6.96, "value": 410, "price": 90},
        {"symbol": "SHB", "change_pct": 0.36, "value": 120, "price": 10},
    ]


def _fake_industry():
    return {
        "VIC": {"icb_code": "8600", "icb_name": "Bất động sản"},
        "VHM": {"icb_code": "8600", "icb_name": "Bất động sản"},
        "SHB": {"icb_code": "8350", "icb_name": "Ngân hàng"},
    }


def test_get_heatmap_groups_and_fields(monkeypatch):
    # Bỏ qua fetch mạng: nạp sẵn danh sách mã + reset memoize bản đồ ngành
    monkeypatch.setattr(vn100_service, "_symbols", ["VIC", "VHM", "SHB"])
    monkeypatch.setattr(sector_service, "_industry_map", {})

    out = sector_service.get_heatmap(board_fn=_fake_board, industry_fn=_fake_industry)

    groups = {g["group"]: g for g in out}
    assert "Bất động sản" in groups
    assert "Ngân hàng" in groups

    bds = groups["Bất động sản"]
    assert bds["icb_code"] == "8600"
    assert len(bds["symbols"]) == 2

    # mỗi mã chỉ còn symbol, change_pct, market_cap (=value)
    sym = {s["symbol"]: s for s in bds["symbols"]}
    assert set(sym["VIC"].keys()) == {"symbol", "change_pct", "market_cap"}
    assert sym["VIC"]["change_pct"] == 6.15
    assert sym["VIC"]["market_cap"] == 480


def test_get_heatmap_market_cap_defaults_zero(monkeypatch):
    monkeypatch.setattr(vn100_service, "_symbols", ["AAA"])
    monkeypatch.setattr(sector_service, "_industry_map", {})

    def board(symbols):
        return [{"symbol": "AAA", "change_pct": 1.0}]  # thiếu 'value'

    def industry():
        return {"AAA": {"icb_code": "1", "icb_name": "X"}}

    out = sector_service.get_heatmap(board_fn=board, industry_fn=industry)
    assert out[0]["symbols"][0]["market_cap"] == 0
