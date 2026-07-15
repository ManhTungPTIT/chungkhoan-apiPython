"""Nguồn realtime DNSE — LightSpeed API **V2** (OpenAPI) qua SDK chính thức `dnse`.

V1 (MQTT `datafeed-lts.dnse.com.vn`, login username/password, topic
`plaintext/quotes/stock/tick/+`) ĐÃ BỊ DNSE NGỪNG HỖ TRỢ → broker trả SUBACK
"Not authorized" cho mọi topic ⇒ không nhận được tick. V2 dùng WebSocket
`wss://ws-openapi.dnse.com.vn`, xác thực bằng **API key/secret** (HMAC-SHA256).
SDK (`pip install dnse-sdk-openapi`) tự lo reconnect + heartbeat + re-subscribe.

Một kết nối cho cả hệ thống. Endpoint /ws/quotes gọi want()/unwant() theo mã FE
đang xem (v2 KHÔNG có wildcard — phải subscribe theo danh sách mã cụ thể). Tick
đã normalize được đẩy vào tick_hub để phân phối cho từng user theo mã họ đăng ký.

Credentials NGOÀI git: env DNSE_API_KEY/DNSE_API_SECRET, hoặc file yaml
(DNSE_CREDS_FILE, mặc định dnse_creds.yaml cạnh module — format {api_key,
api_secret}). Thiếu creds → start_stream() không chạy, log cảnh báo; FE tự
fallback về poll /quotes.
"""

import asyncio
import logging
import os
import time

import yaml

import envfile

logger = logging.getLogger(__name__)

MODULE_DIR = os.path.dirname(__file__)
DEFAULT_CREDS_FILE = os.path.join(MODULE_DIR, "dnse_creds.yaml")

# Mã chỉ số đi qua kênh `market_index` (khác cổ phiếu đi kênh `tick`/trades).
# Danh sách các index DNSE phát; FE mặc định mở VNINDEX.
INDEX_SYMBOLS = {
    "VNINDEX",
    "VN30",
    "VN100",
    "HNXINDEX",
    "HNX30",
    "UPCOMINDEX",
    "VNXALLSHARE",
}


def load_api_creds():
    """(api_key, api_secret) từ env DNSE_API_KEY/DNSE_API_SECRET, không có thì đọc
    yaml {api_key, api_secret} tại DNSE_CREDS_FILE. Thiếu/không đọc được → None."""
    envfile.load_dotenv()
    api_key = os.environ.get("DNSE_API_KEY")
    api_secret = os.environ.get("DNSE_API_SECRET")
    if api_key and api_secret:
        return api_key, api_secret

    path = os.environ.get("DNSE_CREDS_FILE", DEFAULT_CREDS_FILE)
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data["api_key"], data["api_secret"]
    except (OSError, TypeError, KeyError, yaml.YAMLError) as e:
        logger.info("khong nap duoc creds DNSE (%s): %s", path, e)
        return None


def is_index(symbol):
    """Mã có phải chỉ số không (định tuyến kênh market_index vs trades)."""
    return str(symbol or "").upper() in INDEX_SYMBOLS


def _to_unix_seconds(received_at):
    """SDK gắn receivedAt = epoch giây (float) lúc nhận message. Thiếu → 'bây giờ'
    (tick là realtime nên khớp khung nến theo đồng hồ tường)."""
    try:
        return int(float(received_at))
    except (TypeError, ValueError):
        return int(time.time())


def _extract_volume(raw):
    """KL cộng dồn phiên → int; thiếu/NaN thì bỏ qua (quote không kèm volume)."""
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    if v != v:  # NaN
        return None
    return int(v)


def normalize_trade(trade):
    """SDK Trade (kênh tick — cổ phiếu) → quote chuẩn CÙNG shape với /quotes:
    {symbol, price, time, volume?}. price ĐÃ ở nghìn đồng (v2 KHÔNG cần chia
    PRICE_BOARD_SCALE như v1). Thiếu symbol / giá <= 0 → None."""
    symbol = getattr(trade, "symbol", None)
    if not isinstance(symbol, str) or not symbol:
        return None
    try:
        price = float(getattr(trade, "price", None))
    except (TypeError, ValueError):
        return None
    if not (price > 0):  # loại luôn NaN (NaN > 0 là False)
        return None

    quote = {
        "symbol": symbol.upper(),
        "price": price,
        "time": _to_unix_seconds(getattr(trade, "receivedAt", None)),
    }
    volume = _extract_volume(getattr(trade, "totalVolumeTraded", None))
    if volume is not None:
        quote["volume"] = volume
    return quote


def normalize_index(index):
    """SDK MarketIndex (kênh market_index — chỉ số) → quote chuẩn. price =
    valueIndexes (đơn vị ĐIỂM, khớp nến /intraday của index, KHÔNG scale).
    Thiếu tên / giá trị <= 0 → None."""
    name = getattr(index, "indexName", None)
    if not isinstance(name, str) or not name:
        return None
    try:
        price = float(getattr(index, "valueIndexes", None))
    except (TypeError, ValueError):
        return None
    if not (price > 0):
        return None

    quote = {
        "symbol": name.upper(),
        "price": price,
        "time": _to_unix_seconds(getattr(index, "receivedAt", None)),
    }
    volume = _extract_volume(getattr(index, "totalVolumeTraded", None))
    if volume is not None:
        quote["volume"] = volume
    return quote


# ===== quản lý kết nối + subscribe động =====

_client = None
_lock = asyncio.Lock()
_refcounts = {}  # symbol (hoa) -> số WS client đang xem


def _channels_for(symbol):
    """Kênh SDK cần unsubscribe cho một mã (đối xứng với subscribe)."""
    if is_index(symbol):
        return [f"market_index.{symbol}.json"]
    from dnse.websocket.client import DEFAULT_BOARDS

    return [f"tick.{board}.json" for board in DEFAULT_BOARDS]


async def _subscribe_symbol(symbol):
    """Đăng ký một mã trên SDK. Handler đã gắn 1 lần ở start_stream nên truyền
    callback None (chỉ mở kênh, không gắn thêm handler → tránh gọi trùng)."""
    if _client is None:
        return
    try:
        if is_index(symbol):
            await _client.subscribe_market_index(symbol)
        else:
            await _client.subscribe_trades([symbol])
    except Exception as e:  # noqa: BLE001 — lỗi 1 mã không được làm sập stream
        logger.warning("DNSE subscribe %s loi: %s", symbol, e)


async def _unsubscribe_symbol(symbol):
    if _client is None:
        return
    symbols = [] if is_index(symbol) else [symbol]
    for channel in _channels_for(symbol):
        try:
            await _client.unsubscribe(channel, symbols)
        except Exception as e:  # noqa: BLE001
            logger.warning("DNSE unsubscribe %s loi: %s", symbol, e)


async def want(symbol):
    """FE bắt đầu xem `symbol` → tăng refcount, lần đầu (0→1) thì subscribe SDK.
    Nếu client CHƯA connect xong, chỉ ghi refcount; start_stream sẽ subscribe lại
    toàn bộ mã đang want sau khi connect (cũng phục vụ reconnect)."""
    key = str(symbol or "").strip().upper()
    if not key:
        return
    async with _lock:
        n = _refcounts.get(key, 0)
        _refcounts[key] = n + 1
        if n == 0:
            await _subscribe_symbol(key)


async def unwant(symbol):
    """FE rời `symbol` → giảm refcount, lần cuối (→0) thì unsubscribe SDK."""
    key = str(symbol or "").strip().upper()
    if not key:
        return
    async with _lock:
        n = _refcounts.get(key, 0)
        if n <= 0:
            return
        if n > 1:
            _refcounts[key] = n - 1
            return
        _refcounts.pop(key, None)
        await _unsubscribe_symbol(key)


async def start_stream(publish):
    """Mở kết nối SDK V2 + gắn handler tick/index. Không có creds → log + bỏ qua
    (FE dùng poll). Gọi lặp khi đã có client → bỏ qua. Sau khi connect, subscribe
    lại mọi mã đang want (WS client kết nối trước lúc connect xong, hoặc reconnect)."""
    global _client
    creds = load_api_creds()
    if not creds:
        logger.warning(
            "DNSE_API_KEY/DNSE_API_SECRET (hoac %s) chua co — bo qua stream realtime,"
            " FE dung fallback poll /quotes",
            os.environ.get("DNSE_CREDS_FILE", DEFAULT_CREDS_FILE),
        )
        return False
    if _client is not None:
        return True

    from dnse import TradingClient

    api_key, api_secret = creds
    client = TradingClient(api_key, api_secret)

    def on_trade(trade):
        quote = normalize_trade(trade)
        if quote:
            publish(quote)

    def on_index(index):
        quote = normalize_index(index)
        if quote:
            publish(quote)

    client.on("trade", on_trade)
    client.on("market_index", on_index)

    try:
        await client.connect()
    except Exception as e:  # noqa: BLE001 — connect/auth lỗi thì degrade về poll
        logger.warning("DNSE v2 connect loi: %s — FE dung fallback poll /quotes", e)
        return False

    async with _lock:
        _client = client
        for key in list(_refcounts):
            await _subscribe_symbol(key)
    logger.info("DNSE v2 stream san sang (wss://ws-openapi.dnse.com.vn)")
    return True


async def stop_stream():
    """Đóng kết nối SDK + dọn state (dùng lúc shutdown)."""
    global _client
    client = _client
    _client = None
    _refcounts.clear()
    if client is not None:
        try:
            await client.disconnect()
        except Exception as e:  # noqa: BLE001
            logger.info("DNSE v2 disconnect loi (bo qua): %s", e)
