"""Hub phân phối quote realtime: dnse_stream (thread MQTT) publish vào đây,
mỗi WebSocket client là một Subscription nhận quote của ĐÚNG các mã đã đăng ký.

Coalesce tự nhiên bằng mailbox: mỗi connection giữ {symbol: quote MỚI NHẤT};
tick dồn dập giữa hai lần drain chỉ còn bản cuối — đúng semantics "giá mới
nhất", client không bao giờ bị dội hàng trăm message/giây. Nhịp đẩy tối đa do
sender loop của endpoint quyết (FLUSH_INTERVAL_S).

publish() được gọi từ thread MQTT (không phải event loop) → đánh thức drain()
qua loop.call_soon_threadsafe; state chung khóa bằng threading.Lock (các thao
tác đều O(số mã đã sub) và không await trong vùng khóa).
"""

import threading

# Nhịp đẩy tối đa mỗi connection (giây giữa 2 lần flush) — dùng ở sender loop
# của endpoint /ws/quotes; để đây cho cùng chỗ với cơ chế coalesce.
FLUSH_INTERVAL_S = 0.25


class Subscription:
    def __init__(self, loop):
        import asyncio

        self.symbols = set()
        self.mailbox = {}  # symbol -> quote mới nhất chưa gửi
        self.event = asyncio.Event()
        self._loop = loop

    def _wake(self):
        self._loop.call_soon_threadsafe(self.event.set)


class TickHub:
    def __init__(self):
        self._lock = threading.Lock()
        self._latest = {}  # symbol -> quote mới nhất toàn hub
        self._subs = set()
        self._loop = None

    def set_loop(self, loop):
        """Event loop của FastAPI — gọi 1 lần trong lifespan trước khi publish."""
        self._loop = loop

    # ===== phía nguồn (thread MQTT) =====

    def publish(self, quote):
        symbol = quote["symbol"]
        wake = []
        with self._lock:
            self._latest[symbol] = quote
            for sub in self._subs:
                if symbol in sub.symbols:
                    sub.mailbox[symbol] = quote
                    wake.append(sub)
        for sub in wake:
            sub._wake()

    def latest(self, symbol):
        with self._lock:
            return self._latest.get(symbol)

    # ===== phía client (event loop) =====

    def connect(self):
        if self._loop is None:
            raise RuntimeError("TickHub chua duoc set_loop()")
        sub = Subscription(self._loop)
        with self._lock:
            self._subs.add(sub)
        return sub

    def disconnect(self, sub):
        with self._lock:
            self._subs.discard(sub)

    def subscribe(self, sub, symbol):
        """Đăng ký mã; có sẵn quote gần nhất thì nhét luôn vào mailbox để client
        thấy giá ngay không phải đợi tick kế tiếp."""
        symbol = symbol.upper()
        with self._lock:
            sub.symbols.add(symbol)
            latest = self._latest.get(symbol)
            if latest is not None:
                sub.mailbox[symbol] = latest
        if latest is not None:
            sub._wake()

    def unsubscribe(self, sub, symbol):
        symbol = symbol.upper()
        with self._lock:
            sub.symbols.discard(symbol)
            sub.mailbox.pop(symbol, None)

    async def drain(self, sub):
        """Đợi tới khi có quote mới rồi trả toàn bộ mailbox (mỗi mã 1 quote mới
        nhất). Caller tự nhịp lại bằng FLUSH_INTERVAL_S giữa các lần gọi.

        Lặp khi thức dậy mà mailbox rỗng (wake tới sau khi lần drain trước đã
        gom mất quote) — không bao giờ trả mảng rỗng."""
        while True:
            await sub.event.wait()
            with self._lock:
                quotes = list(sub.mailbox.values())
                sub.mailbox.clear()
                sub.event.clear()
            if quotes:
                return quotes


# Hub dùng chung toàn process (api.py + dnse_stream cùng trỏ vào đây)
hub = TickHub()
