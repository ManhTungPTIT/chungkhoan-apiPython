"""Test index_overview_service — chart "CHỈ SỐ CHUNG 3 SÀN" (cột nhóm).

4 nhóm VN INDEX / HN INDEX / UP INDEX / VN30, mỗi nhóm 3 cột:
  - gia_tri_khop_lenh (nghìn tỷ): cộng value THẬT của CP theo sàn (VN30 theo member)
  - diem_tang_giam = diem_hien_tai − diem_dong_cua_phien_truoc
  - pct = (hiện tại − trước)/trước × 100
"""

from app.services import index_overview_service as svc


def test_exchange_values_sum_by_exchange_and_vn30():
    board = [
        {"symbol": "AAA", "value": 10_000_000_000_000},  # HOSE, thuộc VN30
        {"symbol": "BBB", "value": 5_000_000_000_000},   # HNX
        {"symbol": "CCC", "value": 2_000_000_000_000},   # UPCOM
        {"symbol": "DDD", "value": 1_000_000_000_000},   # HOSE, không VN30
    ]
    exch_map = {"AAA": "HOSE", "BBB": "HNX", "CCC": "UPCOM", "DDD": "HSX"}  # HSX→HOSE

    totals = svc.exchange_values_from_board(board, exch_map, vn30_members=["AAA"])

    assert totals["HOSE"] == 11_000_000_000_000  # AAA + DDD (HSX chuẩn hoá HOSE)
    assert totals["HNX"] == 5_000_000_000_000
    assert totals["UPCOM"] == 2_000_000_000_000
    assert totals["VN30"] == 10_000_000_000_000  # chỉ AAA


def test_compute_index_overview_points_change_pct_and_value():
    index_points = {
        "VNINDEX": {"current": 100.0, "prev": 95.0},
        "HNXINDEX": {"current": 90.0, "prev": 95.0},  # giảm
    }
    exchange_values = {
        "HOSE": 19_400_000_000_000,  # 19.4 nghìn tỷ
        "HNX": 700_000_000_000,      # 0.7
    }

    result = svc.compute_index_overview(index_points, exchange_values)
    by_name = {r["ten_san"]: r for r in result["indices"]}

    vn = by_name["VN INDEX"]
    assert vn["diem_hien_tai"] == 100.0
    assert vn["diem_dong_cua_phien_truoc"] == 95.0
    assert vn["diem_tang_giam"] == 5.0
    assert vn["pct"] == 5.26                       # 5/95*100
    assert vn["gia_tri_khop_lenh"] == 19.4         # VND / 1e12

    hn = by_name["HN INDEX"]
    assert hn["diem_tang_giam"] == -5.0
    assert hn["pct"] == -5.26
    assert hn["gia_tri_khop_lenh"] == 0.7


def test_index_without_points_still_shows_value():
    """Index thiếu điểm (vd VN30 nếu nguồn không trả) vẫn hiện cột value, các cột
    điểm/% để None (FE ẩn bar)."""
    result = svc.compute_index_overview(
        index_points={}, exchange_values={"VN30": 1_500_000_000_000}
    )
    by_name = {r["ten_san"]: r for r in result["indices"]}

    vn30 = by_name["VN30"]
    assert vn30["diem_hien_tai"] is None
    assert vn30["diem_tang_giam"] is None
    assert vn30["pct"] is None
    assert vn30["gia_tri_khop_lenh"] == 1.5


def test_all_four_groups_present_in_order():
    result = svc.compute_index_overview({}, {})
    assert [r["ten_san"] for r in result["indices"]] == [
        "VN INDEX", "HN INDEX", "UP INDEX", "VN30",
    ]
