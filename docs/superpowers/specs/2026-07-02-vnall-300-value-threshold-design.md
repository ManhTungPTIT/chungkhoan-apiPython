# Mở rộng VN100 → VNALL + HNX (~593 mã) + hạ ngưỡng lọc giá trị xuống 1 tỷ

**Ngày:** 2026-07-02
**Trạng thái:** Đã duyệt (bổ sung HNX cùng ngày, sau khi phát hiện MBS/IVS/API
nằm ngoài VNALL)

## Mục tiêu

Bảng giá và toàn bộ views liên quan (sectors, heatmap, homepage, tín hiệu) mở
rộng từ rổ VN100 (100 mã) lên rổ VNALL (300 mã HOSE) **+ toàn sàn HNX (~293
mã)**, đồng thời hạ ngưỡng lọc giá trị giao dịch từ 5 tỷ xuống 1 tỷ VND.

## Bối cảnh đã xác minh

- vnstock **không có nhóm "VN300"** — HOSE không có chỉ số VN300 chính thức.
  Nhóm `VNALL` của vnstock trả về **đúng 300 mã** (chỉ số VNAllShare =
  VN100 + VNSmallCap), khớp với ý định "VN300".
- VNALL chỉ gồm HOSE → thiếu các mã HNX (MBS/IVS/API). `VNXALL` (505 mã) vẫn
  thiếu API; chỉ nhóm `HNX` (toàn sàn, 293 mã) có đủ → chọn gộp VNALL + HNX.
- `price_board` với 593 mã chạy tốt trong 1 request (~1.7s, nguồn VCI).
- Với ngưỡng 1 tỷ: ~136 mã lọt bộ lọc giữa phiên (so với ~58 mã ở ngưỡng 5 tỷ
  rổ VN100 cũ). Mã HNX thanh khoản thấp (IVS/API) chỉ hiện khi giá trị giao
  dịch trong ngày vượt 1 tỷ — đúng mục đích bộ lọc.

## Thay đổi

### 1. Nhóm mã — `data_source.py`

`_default_listing()`: `symbols_by_group("VN100")` → gộp
`symbols_by_group("VNALL")` + `symbols_by_group("HNX")`, dedupe giữ thứ tự.
Một trong hai lời gọi lỗi → cả fetch lỗi (trả None, thử lại sau) — tránh
memoize rổ thiếu nửa sàn.
Giữ nguyên tên hàm `fetch_vn100_*`, endpoint `/api/python/vn100`, cache key
`board_vn100` — chỉ cập nhật docstring ghi rõ rổ mới.

### 2. Ngưỡng lọc — `vn100_service.py`

`VALUE_THRESHOLD = 5_000_000_000` → `1_000_000_000`. Cập nhật docstring
`_process` ("Lọc giá trị giao dịch > 1 tỷ").

## Hiệu ứng lan tỏa (tự động, không cần sửa thêm)

- Bảng giá, sectors, heatmap, homepage top-volume đều mở rộng lên ~593 mã vì
  dùng chung `get_symbols()` (memoize module-level).
- signal_service: số mã active tăng ~58 → ~136+ → thời gian tính tín hiệu nền
  tăng từ ~1 phút lên ~3-4 phút/phiên; throttle 1.1s/mã vẫn an toàn rate-limit.
- Server đang chạy cần restart để memoize `_symbols` lấy danh sách mới.
- `signal_cache.json` cũ thiếu tín hiệu các mã mới cho tới lần tính kế tiếp —
  tự lành theo cơ chế sẵn có.

## Testing

- Bật lại `test_get_active_symbols_filters_by_value` (bỏ `@pytest.mark.skip`
  — skip đã lỗi thời vì filter trong `_process` đang bật). Dữ liệu test vẫn
  đúng với ngưỡng mới: B = 1 tỷ không lọt vì điều kiện `>` nghiêm ngặt; chỉ
  sửa comment.
- Chạy toàn bộ pytest xác nhận không vỡ gì.

## Không làm (YAGNI)

- Không rename endpoint/file/hook FE sang "vn300" — giữ tên `vn100` hiện có,
  frontend không cần sửa.
- Không thêm endpoint mới song song.
