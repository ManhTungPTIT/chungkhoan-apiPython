# Thiết kế: Endpoint nhóm ngành hay giao dịch (`/api/python/sectors`)

Ngày: 2026-06-17

## Mục tiêu

Cung cấp **hai endpoint** quanh các **nhóm ngành ICB cấp 3** trong rổ VN100, xếp
theo **tổng giá trị giao dịch** giảm dần ("nhóm ngành hay được giao dịch"):

1. Lấy *danh sách nhóm ngành* (nhẹ) — chỉ tên nhóm, mã ICB, tổng value, số mã.
2. Lấy *các mã của một nhóm* — mỗi mã kèm **OHLC của phiên hôm nay** (các thông số
   giống `fetch_intraday_history` nhưng chỉ một ngày — phiên hiện tại).

Tách hai endpoint chỉ là chia hình dạng response cho FE drill-down; cả hai lấy từ
**cùng một request `price_board` + bản đồ ngành** nên chi phí không đổi.

## Quyết định đã chốt

| Vấn đề | Quyết định |
|--------|-----------|
| Tiêu chí "hay giao dịch" | Tổng giá trị giao dịch (`value`) của các mã trong nhóm |
| Phạm vi mã | Rổ VN100 (tái dùng `fetch_vn100_symbols`, đã memoize) |
| Cấp gom nhóm | ICB **cấp 3** (vd: "Bảo hiểm nhân thọ", "Ngân hàng") |
| OHLC từng mã | Phiên **hôm nay**, lấy từ chính `price_board` (không gọi history) |
| Ngày yêu cầu | Luôn là phiên hiện tại |
| Cấu trúc API | Tách 2 endpoint: danh sách nhóm + mã của một nhóm |
| Tham số chọn nhóm | `icb_code` (ổn định, không lệ thuộc encoding tiếng Việt) |

## Luồng dữ liệu

```
VN100 symbols (memoize) ─┐
                         ├─► price_board(symbols)  [1 request]
industry map (memoize) ──┘        → board: {symbol, value, open, high, low, close, price, change_pct}
        │
        │   symbols_by_industries(VCI), lọc icb_level==3
        │        → {symbol: {icb_code, icb_name}}
        ▼
   join board × industry map → gom theo icb_name (cấp 3)
        │   tổng value mỗi nhóm; sort nhóm theo total_value desc;
        │   sort mã trong nhóm theo value desc
        ▼
   {data: [ {group, icb_code, total_value, symbols:[...]} ]}
```

Toàn bộ chỉ cần **1 request `price_board`** (đã chứa OHLC + accumulated_value) cộng
**1 lần lấy bản đồ ngành** (memoize, hiếm đổi). Không gọi history từng mã → tránh
rate-limit.

## Các thành phần

### `data_source.py` (điểm duy nhất chạm vnstock)

- Mở rộng `_map_board` thêm 4 trường OHLC (cộng thêm, không phá `/vn100` cũ —
  thay đổi tương thích ngược vì chỉ thêm khóa):
  ```python
  "open":  row[("match", "open_price")],
  "high":  row[("match", "highest")],
  "low":   row[("match", "lowest")],
  "close": row[("match", "match_price")],   # = "price" hiện có
  ```
  → `fetch_vn100_board` tự động có OHLC, dùng lại cho cả hai endpoint (một nguồn sự thật).

- `_default_industries()` → `Listing(source="VCI").symbols_by_industries()`; lọc
  `icb_level == 3`; trả `dict {symbol: {"icb_code": str, "icb_name": str}}`.

- `fetch_industry_map(industries_fn=_default_industries) -> Optional[dict]` — bọc
  `BaseException`, lỗi (kể cả rate-limit/SystemExit) → `None`.

### `sector_service.py` (mới)

- `_industry_map: dict` — biến module-level, memoize giống `_symbols` trong
  `vn100_service` (phân loại ngành hiếm đổi; `None`/rỗng thì lần sau tự thử lại — self-heal).
- `get_industry_map(fetch_fn=data_source.fetch_industry_map) -> dict` — lấy 1 lần rồi giữ.
- `_build_groups(board, imap) -> list[dict]` (nội bộ) — xây cấu trúc đầy đủ
  `[{group, icb_code, total_value, symbols:[...]}]`: mỗi mã tra nhóm cấp 3 trong
  `imap` (thiếu → `"Chưa phân loại"`, `icb_code` rỗng); gom theo `icb_code`, cộng
  `total_value`; sort nhóm theo `total_value` desc; trong nhóm sort mã theo `value` desc.
- `get_sectors(board_fn=data_source.fetch_vn100_board, industry_fn=...) -> dict` —
  trả **danh sách nhóm, bỏ chi tiết mã**: chiếu mỗi nhóm thành
  `{group, icb_code, total_value, symbol_count}`.
- `get_sector_symbols(icb_code, board_fn=..., industry_fn=...) -> dict` — trả
  **các mã + OHLC của một nhóm**: lọc nhóm trùng `icb_code`. Không thấy → `{"group": None, "icb_code": icb_code, "data": []}`.

Cả hai endpoint gọi `_build_groups` (cùng 1 `price_board` + bản đồ ngành) rồi
chiếu ra phần mình cần — không gọi vnstock thêm.

Quy ước chung khi không lấy được dữ liệu: `symbols` rỗng / `board is None` →
`{"data": []}`; không bao giờ 500.

### `api.py`

```python
@app.get("/api/python/sectors")
def get_sectors():
    return sector_service.get_sectors()

@app.get("/api/python/sectors/symbols")
def get_sector_symbols(icb_code: str = Query(description="Mã ICB cấp 3, vd 8350")):
    return sector_service.get_sector_symbols(icb_code)
```

## Hình dạng response

`GET /api/python/sectors` — danh sách nhóm (nhẹ):

```json
{
  "data": [
    {"group": "Ngân hàng", "icb_code": "8350", "total_value": 1234000000000, "symbol_count": 12},
    {"group": "Bảo hiểm nhân thọ", "icb_code": "8570", "total_value": 89000000000, "symbol_count": 1}
  ]
}
```

`GET /api/python/sectors/symbols?icb_code=8350` — mã + OHLC của một nhóm:

```json
{
  "group": "Ngân hàng", "icb_code": "8350",
  "data": [
    {"symbol": "TCB", "open": 24.5, "high": 25.0, "low": 24.3, "close": 24.8,
     "price": 24.8, "change_pct": 1.2, "value": 500000000000}
  ]
}
```

## Xử lý lỗi & biên

- Mọi fetch hỏng → `None` tại `data_source`; service trả `{"data": []}`. Không bao
  giờ trả 500 (đúng triết lý hiện tại của repo).
- Mã VN100 thiếu ICB cấp 3 → gom vào `"Chưa phân loại"` (không loại bỏ mã).
- **Không** áp ngưỡng 5 tỷ ở đây (khác mục đích với `/vn100`); xếp hạng dựa trên
  toàn bộ value của nhóm. Có thể thêm lọc thanh khoản sau nếu cần.

## Kiểm thử

- Repo hiện chưa có test. Giữ tham số inject (`board_fn`, `industry_fn`, `fetch_fn`)
  để có thể tiêm fake.
- Tự chạy thật endpoint một lần (gọi vnstock thật) để xác nhận cấu trúc response
  trước khi báo hoàn thành.

## Ngoài phạm vi (YAGNI)

- Không hỗ trợ chọn ngày quá khứ.
- Không hỗ trợ phạm vi ngoài VN100 (toàn sàn).
- Không cache board (giữ nguyên triết lý gọi trực tiếp); chỉ memoize bản đồ ngành.
