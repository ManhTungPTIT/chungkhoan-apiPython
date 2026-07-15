# Spec: Migrate realtime DNSE v1 → v2 (OpenAPI / LightSpeed API V2)

Ngày: 2026-07-15

## Vấn đề
Realtime chart trễ ~10s cho MỌI mã (cổ phiếu lẫn chỉ số). Root cause (đã xác minh
live): code dùng **LightSpeed API v1 đã bị DNSE ngừng hỗ trợ** — MQTT tới
`datafeed-lts.dnse.com.vn`, login username/password, topic `plaintext/quotes/stock/tick/+`.
Broker trả `SUBACK Not authorized` cho mọi topic ⇒ `tick_hub` không nhận tick ⇒
`/ws/quotes` đẩy 0 message ⇒ FE rơi về poll `/quotes` (BE snapshot cũ ~10s + FE poll 5s).

## Giải pháp
Chuyển sang **LightSpeed API V2** qua SDK chính thức `dnse` (`pip install
dnse-sdk-openapi`): WebSocket `wss://ws-openapi.dnse.com.vn`, xác thực **API
key/secret** (HMAC-SHA256), SDK tự lo reconnect + heartbeat + re-subscribe.

Đã xác minh live (2026-07-15, thị trường mở): SDK connect + auth OK, nhận 43
trade/15s cho SSI/HPG/FPT và VNINDEX realtime qua `subscribe_market_index`.

## Khác biệt v2 đã kiểm chứng
1. `Trade.price` (=matchPrice) ĐÃ ở đơn vị **nghìn đồng** (HPG=22.3) → KHÔNG chia
   `PRICE_BOARD_SCALE` nữa (v1 chia). Khớp thẳng đơn vị nến FE.
2. **VNINDEX có realtime** qua kênh `market_index.VNINDEX.json` (`valueIndexes`=điểm)
   → bỏ được độ trễ cho chỉ số (trước đây chỉ số luôn phải poll).
3. `subscribe_trades(symbols=[])` = KHÔNG có gì (không phải "tất cả") → phải subscribe
   theo danh sách mã cụ thể ⇒ dùng **subscribe động theo nhu cầu** (refcount).

## Kiến trúc
```
TradingClient (async, chạy như asyncio task trong FastAPI loop)
  ├─ on("trade")        → normalize_trade → hub.publish({symbol, price, time, volume})
  └─ on("market_index") → normalize_index → hub.publish({symbol, price, time, volume})
        ↓ hub định tuyến theo mã (GIỮ NGUYÊN)
     /ws/quotes → FE useQuoteStream (GIỮ NGUYÊN)
```

`dnse_stream` giữ một `TradingClient` + refcount `{symbol: count}` (asyncio.Lock):
- `want(symbol)`: count 0→1 → subscribe (chỉ số → `subscribe_market_index`; cổ phiếu →
  `subscribe_trades([symbol])`). Handler đăng ký MỘT LẦN lúc connect (tránh trùng).
- `unwant(symbol)`: count →0 → unsubscribe (chỉ số: kênh market_index; cổ phiếu: mọi
  board `tick.{board}.json`).
- Want tới trước khi client connect xong → chỉ ghi refcount; `start_stream` sau khi
  connect sẽ subscribe lại toàn bộ mã đang want (cũng phục vụ reconnect).

## Điểm nối
- `api.py` lifespan: `asyncio.create_task(dnse_stream.start_stream(hub.publish))`
  (không block startup); shutdown: `await dnse_stream.stop_stream()`.
- `/ws/quotes`: sau `hub.subscribe` → `await dnse_stream.want(symbol)`; sau
  `hub.unsubscribe` → `await dnse_stream.unwant(symbol)`; lúc disconnect release mọi
  mã đã want.

## Config
- Creds: env `DNSE_API_KEY` / `DNSE_API_SECRET` (hoặc yaml `{api_key, api_secret}`).
  Thiếu → không chạy stream, FE fallback poll (như cũ).

## Không đổi
`tick_hub.py`, route `/ws/quotes` (chỉ thêm want/unwant), TOÀN BỘ FE — shape quote
`{symbol, price, time, volume?}` giữ nguyên.

## Test
- Unit offline: `normalize_trade`, `normalize_index` (đơn vị giá, loại giá ≤0/thiếu
  symbol, volume tùy chọn), `load_api_creds` (env / yaml / thiếu).
- Verify live: BE thật + WS client subscribe SSI + VNINDEX thấy tick chảy.

## Loại bỏ
Code v1: `_dnse_login`, `account_info`, paho MQTT, `normalize_tick`, backoff thủ công,
`on_subscribe`/`_log_subscribe_result` (SUBACK là khái niệm MQTT, v2 không có).
`requirements.txt`: bỏ `paho-mqtt`, thêm `dnse-sdk-openapi` + `msgpack`.
