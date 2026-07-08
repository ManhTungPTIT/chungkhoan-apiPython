"""Kết nối nguồn realtime DNSE LightSpeed (MQTT over WebSocket) — MỘT kết nối
cho cả hệ thống, nhận tick khớp lệnh mọi mã rồi đẩy vào tick_hub để endpoint
/ws/quotes phân phối cho từng user theo mã họ subscribe.

Xác thực bằng TÀI KHOẢN DNSE (không phải API key vnstock): đăng nhập lấy JWT
(hiệu lực ~8 tiếng) + investorId; MQTT username=investorId, password=JWT.
JWT hết hạn → broker ngắt → vòng kết nối tự đăng nhập lại lấy JWT mới rồi nối
lại với backoff (không giữ client cũ vì password đã chết).

Credentials nằm NGOÀI git: env DNSE_USER/DNSE_PASSWORD, hoặc file yaml
(DNSE_CREDS_FILE, mặc định dnse_creds.yaml cạnh module — format {usr, pwd}
giống connector dnse của vnstock_data). Thiếu creds → start_stream() không
chạy gì, log cảnh báo; FE tự fallback về poll /quotes 5s.
"""

import datetime as dt
import json
import logging
import os
import random
import threading
import time

import requests
import yaml

import signal_service

logger = logging.getLogger(__name__)

AUTH_URL = "https://services.entrade.com.vn/dnse-user-service/api/auth"
ME_URL = "https://services.entrade.com.vn/dnse-user-service/api/me"
BROKER_HOST = "datafeed-lts.dnse.com.vn"
BROKER_PORT = 443
BROKER_WS_PATH = "/wss"
# Tick khớp lệnh của TẤT CẢ cổ phiếu trong một subscription (wildcard +)
TICK_TOPIC = "plaintext/quotes/stock/tick/+"

DEFAULT_CREDS_FILE = os.path.join(os.path.dirname(__file__), "dnse_creds.yaml")

FIRST_RECONNECT_DELAY_S = 1
MAX_RECONNECT_DELAY_S = 60


def load_creds():
    """(username, password) từ env DNSE_USER/DNSE_PASSWORD, không có thì đọc
    yaml {usr, pwd} tại DNSE_CREDS_FILE. Thiếu/không đọc được → None."""
    user = os.environ.get("DNSE_USER")
    password = os.environ.get("DNSE_PASSWORD")
    if user and password:
        return user, password

    path = os.environ.get("DNSE_CREDS_FILE", DEFAULT_CREDS_FILE)
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data["usr"], data["pwd"]
    except (OSError, TypeError, KeyError, yaml.YAMLError) as e:
        logger.info("khong nap duoc creds DNSE (%s): %s", path, e)
        return None


def _tick_time_to_unix_seconds(value):
    """Tick DNSE có thể mang time dạng epoch giây, epoch mili-giây hoặc chuỗi
    ISO — chuẩn hóa về unix giây. Không parse được → None (bỏ tick)."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        v = float(value)
        return int(v / 1000) if v > 1e12 else int(v)
    if isinstance(value, str):
        try:
            return int(dt.datetime.fromisoformat(value).timestamp())
        except ValueError:
            return None
    return None


def normalize_tick(payload):
    """Tick DNSE → quote chuẩn của hệ thống, CÙNG shape với /quotes:
    {symbol, price (nghìn đồng — matchPrice VND / PRICE_BOARD_SCALE),
     volume (KL cộng dồn phiên, bỏ qua nếu thiếu), time (unix giây)}.
    Tick thiếu/hỏng field bắt buộc hoặc giá <= 0 → None."""
    if not isinstance(payload, dict):
        return None
    symbol = payload.get("symbol")
    if not isinstance(symbol, str) or not symbol:
        return None
    try:
        price = float(payload.get("matchPrice"))
    except (TypeError, ValueError):
        return None
    if not (price > 0):  # loại luôn NaN (NaN > 0 là False)
        return None
    tick_time = _tick_time_to_unix_seconds(payload.get("time"))
    if tick_time is None:
        return None

    quote = {
        "symbol": symbol.upper(),
        "price": price / signal_service.PRICE_BOARD_SCALE,
        "time": tick_time,
    }
    try:
        volume = float(payload.get("volume"))
        if volume == volume:  # not NaN
            quote["volume"] = int(volume)
    except (TypeError, ValueError):
        pass
    return quote


def _dnse_login(user, password):
    """Đăng nhập DNSE → (investor_id, jwt). Lỗi HTTP → raise để vòng kết nối
    backoff rồi thử lại."""
    resp = requests.post(
        AUTH_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({"username": user, "password": password}),
        timeout=15,
    )
    resp.raise_for_status()
    token = resp.json()["token"]
    me = requests.get(
        ME_URL,
        headers={"Content-Type": "application/json", "authorization": f"Bearer {token}"},
        timeout=15,
    )
    me.raise_for_status()
    return me.json()["investorId"], token


def _run_stream_forever(user, password, on_quote, stop_event):
    """Vòng đời kết nối: login → MQTT connect → loop tới khi rớt → backoff →
    login lại (JWT mới). Mỗi chu kỳ tạo client MỚI vì JWT cũ có thể đã hết hạn."""
    from paho.mqtt import client as mqtt_client  # import trễ: test offline không cần paho
    from paho.mqtt.client import MQTTv5
    from paho.mqtt.subscribeoptions import SubscribeOptions

    delay = FIRST_RECONNECT_DELAY_S
    while not stop_event.is_set():
        try:
            investor_id, token = _dnse_login(user, password)

            def on_connect(client, userdata, flags, rc, properties=None):
                if rc == 0:
                    logger.info("DNSE MQTT connected — subscribe %s", TICK_TOPIC)
                    client.subscribe([(TICK_TOPIC, SubscribeOptions(qos=1))])
                else:
                    logger.warning("DNSE MQTT connect rc=%s", rc)

            def on_message(client, userdata, msg):
                try:
                    quote = normalize_tick(json.loads(msg.payload.decode()))
                except (ValueError, UnicodeDecodeError):
                    return
                if quote:
                    on_quote(quote)

            client = mqtt_client.Client(
                client_id=f"vnstock-realtime-{random.randint(0, 100000)}",
                protocol=MQTTv5,
                transport="websockets",
            )
            client.username_pw_set(investor_id, token)
            client.tls_set_context()
            client.ws_set_options(path=BROKER_WS_PATH)
            client.on_connect = on_connect
            client.on_message = on_message

            client.connect(BROKER_HOST, BROKER_PORT, keepalive=120)
            delay = FIRST_RECONNECT_DELAY_S  # nối được → reset backoff
            # loop_forever tự retry ở tầng TCP nhưng KHÔNG làm mới JWT; rớt hẳn
            # (return/raise) thì ra ngoài để login lại với token mới.
            client.loop_forever(retry_first_connection=False)
        except Exception as e:  # noqa: BLE001 — lỗi mạng/auth nào cũng chỉ backoff rồi thử lại
            logger.warning("DNSE stream loi: %s — thu lai sau %ss", e, delay)
        if stop_event.is_set():
            return
        time.sleep(delay)
        delay = min(delay * 2, MAX_RECONNECT_DELAY_S)


_stream_thread = None
_stop_event = threading.Event()


def start_stream(on_quote):
    """Chạy stream DNSE ở thread nền (daemon). Không có creds → log + bỏ qua
    (hệ thống vẫn chạy với poll như cũ). Gọi lặp lại khi thread đang sống → bỏ qua."""
    global _stream_thread
    creds = load_creds()
    if not creds:
        logger.warning(
            "DNSE_USER/DNSE_PASSWORD (hoac %s) chua co — bo qua stream realtime,"
            " FE dung fallback poll /quotes",
            os.environ.get("DNSE_CREDS_FILE", DEFAULT_CREDS_FILE),
        )
        return False
    if _stream_thread is not None and _stream_thread.is_alive():
        return True
    _stop_event.clear()
    _stream_thread = threading.Thread(
        target=_run_stream_forever,
        args=(*creds, on_quote, _stop_event),
        name="dnse-stream",
        daemon=True,
    )
    _stream_thread.start()
    return True
