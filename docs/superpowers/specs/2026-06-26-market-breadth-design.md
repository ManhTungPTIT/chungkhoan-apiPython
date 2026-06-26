# Market Breadth — số mã tăng/giảm/đứng giá + KL khớp phiên hôm qua

## Mục tiêu
Thêm endpoint homepage cho biết "độ rộng thị trường": số mã tăng / giảm / đứng
giá so với hôm qua (real-time), kèm tổng khối lượng khớp lệnh phiên hôm qua.

## Endpoint
`GET /api/python/homepage/market-breadth`

```json
{ "advancers": 0, "decliners": 0, "unchanged": 0, "total_value": 0, "prev_total_volume": 0 }
```

- `total_value` — tổng GIÁ TRỊ khớp lệnh phiên ĐANG diễn ra (real-time, VND),
  cộng `accumulated_value` mọi mã từ chính board đã fetch cho breadth (0 request
  thêm). Index history không có cột value nên không lấy được value phiên hôm qua
  rẻ; do đó value lấy real-time hôm nay, còn `prev_total_volume` là hôm qua.

## Luồng dữ liệu (2 nguồn độc lập, đều fail-safe về 0)

### A. Số mã tăng/giảm/đứng giá (real-time, toàn thị trường)
1. `data_source.fetch_all_symbols()` → toàn bộ mã niêm yết.
2. `data_source.fetch_vn100_board(symbols)` (1 request `price_board`, hàm generic
   đã dùng chung) → list `{symbol, price, change_pct, ...}`; `change_pct` tính từ
   `ref_price` = giá đóng cửa hôm qua.
3. Đếm trong `homepage_service.get_market_breadth`:
   - `price` rỗng/0 (chưa khớp lệnh) → **đứng giá** (không để rơi vào nhánh giảm:
     `_map_board` cho `change_pct = -100` khi `price = 0`).
   - `change_pct > 0` → tăng; `< 0` → giảm; `== 0` → đứng giá.

### B. Tổng KL khớp lệnh phiên hôm qua (cộng 3 index)
1. Thêm `data_source.fetch_prev_session_volume(symbol, history_fn, today)`:
   - Lấy nến `1D` ~10 ngày gần nhất qua `fetch_intraday_history`.
   - Chọn nến có ngày **< hôm nay** gần nhất (= "phiên hôm qua").
   - Trả `volume` (số cổ phiếu, ép int qua `_num`). Lỗi fetch → `None`;
     không có nến phù hợp → `0`.
2. `homepage_service._prev_total_volume()` cộng volume của
   `VNINDEX` + `HNXINDEX` + `UPCOMINDEX` (bỏ qua index nào trả `None`).

## Thay đổi file
- `data_source.py`: thêm `fetch_prev_session_volume(...)` (tái dùng
  `fetch_intraday_history`, `_num`, `import datetime`).
- `homepage_service.py`: thêm `get_market_breadth()` + `_prev_total_volume()` +
  hằng `_MARKET_INDICES`.
- `api.py`: route `/api/python/homepage/market-breadth`.

## Đã verify thực tế với vnstock (VCI)
- Symbol index đúng: `VNINDEX`, `HNXINDEX`, `UPCOMINDEX` (các biến thể
  `HNX-INDEX` / `UPINDEX` báo lỗi).
- Index history có cột `volume`. Mẫu phiên 2026-06-25: VNINDEX 490,618,259 +
  HNXINDEX 44,934,291 + UPCOMINDEX 28,940,411 = `prev_total_volume` 564,492,961.
- Endpoint chạy end-to-end: `{advancers, decliners, unchanged, prev_total_volume}`.
  Lưu ý `unchanged` cao do nhiều mã UPCOM kém thanh khoản chưa khớp (price=0 →
  đứng giá), đúng thiết kế.

## Test (offline, dependency-injection theo pattern sẵn có)
- `fetch_prev_session_volume`: chọn đúng nến < hôm nay; bỏ nến hôm nay;
  không có nến → 0; fetch lỗi → None; volume ép về int.
- `get_market_breadth`: đếm tăng/giảm/đứng; price=0 → đứng giá; cộng đúng 3 index;
  fail-safe (board None → 0/0/0, một index None → vẫn cộng phần còn lại).
