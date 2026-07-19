# Tách request price_board + chiến lược nguồn dữ liệu VCI/ASEAN

> Cập nhật cuối ngày 19/07/2026 — spec ban đầu chỉ có "chia theo sàn"; sau sự
> cố timeout diện rộng (anti-bot VCI, xác nhận với hỗ trợ vnstock) thiết kế đã
> tiến hoá qua 5 bước, tài liệu này mô tả TRẠNG THÁI CUỐI + lý do từng quyết
> định. Chi tiết sự cố: xem memory `vci-antibot-asean-source`.

## Vấn đề gốc

`data_source._default_price_board(symbols)` gọi `Trading(VCI).price_board()`
với TOÀN BỘ ~1600 mã trong 1 request. Hệ quả (xác nhận từ hỗ trợ vnstock
19/07): payload lớn làm backend VCI tính realtime quá 30s (timeout CỨNG của
vnstock_data, không giảm được — chỉ giảm được RETRIES, xem
`vnstock_license.patch_vnstock_data_retries`), và gọi lặp liên tục bị nhận
diện bot → VCI chặn IP tạm thời (mọi price_board treo 30s, kể cả 3 mã).

Hàm này là điểm nối chung (`price_board_fn`) cho `fetch_vn100_board`,
`fetch_market_bid_ask`, `fetch_market_snapshot` — chạy mỗi tick 1-5s trong
giờ GD qua `market_refresher.refresh_tick`.

## Thiết kế cuối cùng

### 1. Chia mẻ: theo sàn, rồi ≤500 mã/mẻ (`PRICE_BOARD_CHUNK_MAX`)

- Bản đồ symbol→sàn (memoize theo ngày, `_symbol_exchange_map`) chia mã theo
  sàn; mã không tra được sàn vào nhóm `"unknown"` (vẫn được fetch, không rớt).
- Sàn >500 mã chia tiếp thành mẻ CÂN BẰNG (`_split_chunks`: UPCOM ~914 →
  2×~457, không phải 500+414) — 500 là trần vendor khuyến nghị.
- Nhãn mẻ dùng cho log: `HOSE`, `HNX`, `UPCOM[1/2]`, `unknown[3/4]`...

### 2. Chạy song song (ThreadPoolExecutor, mỗi mẻ 1 luồng)

Vendor call là I/O đồng bộ — tuần tự cộng dồn latency 4-5 lần; song song chỉ
tốn thời gian mẻ chậm nhất. Không đổi tổng số call/phút (~303, dưới hạn
Golden 500 — xem comment ngân sách đầu `market_refresher.py`).

### 3. Gộp từng phần + last-good

- Mẻ lỗi → bỏ qua (log warning), các mẻ thành công vẫn gộp trả về.
- TẤT CẢ mẻ lỗi → raise, hàm `fetch_*` bao ngoài bắt và trả None →
  `market_cache` giữ snapshot tốt gần nhất (không bao giờ trắng trang).
- Không có retry riêng trong hàm: tick kế tiếp (1-5s) tự fetch lại từ đầu.

### 4. Log đo lường (`SLOW_FETCH_LOG_S = 3.0`)

- Fetch (price_board mẻ / history) vượt 3s → WARNING kèm giây + số mã.
- Fetch lỗi → WARNING kèm THỜI GIAN ĐÃ CHỜ: phân biệt fail-nhanh (rate-limit
  từ chối ngay) với treo-tới-timeout ~30s (server nghẽn/anti-bot) — mấu chốt
  chẩn đoán từ xa qua log.

### 5. Phân bổ nguồn dữ liệu VCI vs ASEAN (probe 19/07, vnstock_data ≥3.2.5)

| Luồng | Nguồn | Lý do |
|---|---|---|
| price_board (giá khớp/KL/bid-ask realtime) | **VCI** (bắt buộc) | ASEAN price_board chỉ có 5 cột tĩnh (trần/sàn/tham chiếu) — không thay được |
| history (nến, `_default_history`) | **ASEAN** | Cùng shape OHLCV, tách ~55 call/phút (quét tín hiệu) khỏi host VCI bị chặn |
| Bản đồ symbol→sàn (`_default_exchange_listing`) | **ASEAN** `price_board()` | Listing VCI `symbols_by_exchange` sập/treo 30s hàng loạt tối 19/07; ASEAN trả symbol+exchange toàn TT ~1s. Nhãn sàn ASEAN là `HOSE` (VCI là `HSX`) — chỉ là nhãn nhóm/log |
| Các listing khác (rổ theo dõi, all_symbols, ICB, VN100) | **VCI** | `Listing` không nhận source ASEAN (chỉ VCI/KBS/VND/CAFEF); call nhẹ 1 lần/ngày, có memoize + last-good |

### 6. Deploy: tự nâng cấp vnstock_data theo bản pin

`vnstock_license.ensure_vnstock_data()` (chạy lúc khởi động) so version đã
cài (metadata pip — KHÔNG dùng `vnstock_data.__version__` vì chuỗi đó stale)
với pin `VNSTOCK_DATA_VERSION = "3.2.5"`; lệch → tải bản pin qua vnii và cài
lại NGAY boot đó, TRƯỚC khi import (tránh sys.modules giữ bản cũ). Deploy chỉ
cần `git pull` + restart, điều kiện duy nhất là `.env` có `VNSTOCK_API_KEY`.

## Ràng buộc giữ nguyên từ spec gốc

- KHÔNG đổi signature `fetch_vn100_board` / `fetch_market_bid_ask` /
  `fetch_market_snapshot` / `fetch_quotes_direct` hay call site nào ở
  `market_refresher.py` — mọi thay đổi khoanh trong `_default_price_board`
  và các `_default_*` factory.
- Test inject `price_board_fn`/`history_fn`/`listing_fn` trực tiếp không đổi.

## Vận hành khi VCI bị chặn/sập

- Hệ thống tự đứng vững bằng last-good; KHÔNG cần can thiệp code.
- Muốn IP được nhả: TẮT server ngừng nã request (vài chục phút+), probe kiểm
  tra bằng script 1 history + 1 price_board 3 mã (xem memory) trước khi bật.
- Nếu vào phiên vẫn timeout dày với mẻ ≤500 → gửi log đo lường cho hỗ trợ
  vnstock làm bằng chứng.

## Lịch sử commit

`1a9f48a` spec gốc → `cc08afd` bản đồ sàn → `9bfab71` chia theo sàn →
`2619113` comment ngân sách → `48f8b97` song song → `7f58d56` log đo lường →
`b2ed0f4` mẻ ≤500 → `68b6218` history ASEAN + pin 3.2.5 → `29638c8`+`f2a43e1`
tự nâng cấp theo pin → `ac270f0` bản đồ sàn ASEAN.
