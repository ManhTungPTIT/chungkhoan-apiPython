# Tách request price_board theo sàn (HSX/HNX/UPCOM)

## Vấn đề

`data_source._default_price_board(symbols)` gọi `Trading(...).price_board(symbols)`
với TOÀN BỘ danh sách mã trong 1 request. Với danh sách toàn thị trường
(`market_refresher._all_symbols_today()`, ~1600 mã), request này dễ timeout/lỗi
do payload quá lớn.

Hàm này là điểm nối chung (`price_board_fn`) cho `fetch_vn100_board`,
`fetch_market_bid_ask`, `fetch_market_snapshot` — được gọi mỗi tick 1s trong
giờ giao dịch qua `market_refresher.refresh_tick`.

## Giải pháp

Tách bên trong `_default_price_board()` — không đổi signature của bất kỳ hàm
`fetch_*` nào hay call site nào ở `market_refresher.py`.

1. **Bản đồ symbol → exchange**: cache memoize theo ngày (cùng pattern với
   `_all_symbols` ở `market_refresher.py`), nạp từ
   `Listing(source=VCI).symbols_by_exchange()` (cột `symbol`, `exchange`).
2. **Chia nhóm**: chia `symbols` đầu vào thành các nhóm theo `exchange` trong
   `{"HSX", "HNX", "UPCOM"}`. Mã không tra được sàn (không có trong bản đồ,
   ví dụ niêm yết mới trước khi cache ngày cập nhật) → nhóm `"unknown"`, vẫn
   được gửi request riêng, không bị rớt khỏi kết quả.
3. **Gọi từng nhóm**: với mỗi nhóm không rỗng, gọi
   `Trading(symbol=group[0], source=VCI).price_board(group)`.
4. **Gộp kết quả — trả từng phần khi lỗi một phần**:
   - Nhóm nào lỗi (exception) → log warning, bỏ qua nhóm đó, KHÔNG làm fail
     toàn bộ.
   - Các nhóm thành công được gộp bằng `pd.concat`.
   - Nếu TẤT CẢ nhóm đều lỗi → raise để hàm gọi ngoài (`fetch_market_snapshot`
     v.v.) bắt exception như cũ và trả `None` (giữ nguyên hành vi last-good
     hiện tại ở `market_cache`).
   - Nếu tất cả nhóm trả rỗng (không lỗi nhưng DataFrame rỗng) → trả DataFrame
     rỗng như hành vi hiện tại.

## Ảnh hưởng ngân sách call

`refresh_tick` hiện gọi 1 price_board/tick (nhịp `QUOTES_INTERVAL_S=5s` hoặc
1s tùy cấu hình). Tách 3 sàn → 3 request/tick thay vì 1 cho phần price_board,
tăng khoảng gấp 3 số call cho riêng phần này. Vẫn dưới hạn Golden 500
req/phút. Comment ngân sách ở đầu `market_refresher.py` (dòng 7-13) cần cập
nhật lại số liệu cho khớp.

## Testing

- Test hiện tại của `fetch_market_snapshot`/`fetch_vn100_board`/
  `fetch_market_bid_ask` inject `price_board_fn` trực tiếp → không đổi hành vi,
  không cần sửa.
- Thêm test mới cho `_default_price_board` (mock `Trading`-like callable +
  mock `listing_fn` trả bản đồ exchange giả):
  - Chia đúng nhóm theo sàn.
  - Gộp đúng dữ liệu khi tất cả nhóm thành công.
  - Gộp từng phần khi 1 nhóm lỗi (dữ liệu 2 nhóm còn lại vẫn có mặt).
  - Trả lỗi (raise) khi tất cả nhóm đều lỗi.
  - Mã không tra được sàn vẫn được gửi trong nhóm "unknown", không bị rớt.

## Ngoài phạm vi

- Không đổi cách `market_refresher._all_symbols_today()` lấy danh sách mã
  (vẫn dùng `fetch_all_symbols`/`Listing.all_symbols()` phẳng, không kèm
  exchange).
- Không đổi logic memoize `_all_symbols` hiện có ở `market_refresher.py`.
