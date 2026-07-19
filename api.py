"""FastAPI app Ã¢â‚¬â€ endpoint Ã„ÂÃ¡Â»Å’C tÃ¡Â»Â« market_cache (snapshot dÃƒÂ¹ng chung), KHÃƒâ€NG gÃ¡Â»Âi
vnstock theo tÃ¡Â»Â«ng request.

LuÃ¡Â»â€œng market_refresher fetch nÃ¡Â»Ân vÃƒÂ o cache theo chu kÃ¡Â»Â³; mÃ¡Â»Âi user Ã„â€˜Ã¡Â»Âc cÃƒÂ¹ng mÃ¡Â»â„¢t
snapshot nÃƒÂªn sÃ¡Â»â€˜ call vnstock khÃƒÂ´ng phÃ¡Â»Â¥ thuÃ¡Â»â„¢c sÃ¡Â»â€˜ user (nÃƒÂ© rate-limit khi Ã„â€˜Ã„Æ’ng nhÃ¡ÂºÂ­p
Ã„â€˜Ã¡Â»â€œng loÃ¡ÂºÂ¡t). Cache cÃƒÂ²n trÃ¡Â»â€˜ng (vÃƒÂ i giÃƒÂ¢y Ã„â€˜Ã¡ÂºÂ§u sau boot) hoÃ¡ÂºÂ·c luÃ¡Â»â€œng nÃ¡Â»Ân chÃ¡ÂºÂ¿t Ã¢â€ â€™ endpoint
tÃ¡Â»Â± fallback gÃ¡Â»Âi service trÃ¡Â»Â±c tiÃ¡ÂºÂ¿p Ã„â€˜Ã¡Â»Æ’ khÃƒÂ´ng bao giÃ¡Â»Â trÃ¡ÂºÂ£ trÃ¡ÂºÂ¯ng.

LÃƒÂºc khÃ¡Â»Å¸i Ã„â€˜Ã¡Â»â„¢ng warm danh sÃƒÂ¡ch mÃƒÂ£ VN100 (memoize). data_source vÃ¡ÂºÂ«n bÃ¡ÂºÂ¯t
BaseException nÃƒÂªn endpoint khÃƒÂ´ng bao giÃ¡Â»Â trÃ¡ÂºÂ£ 500 vÃƒÂ¬ lÃ¡Â»â€”i dÃ¡Â»Â¯ liÃ¡Â»â€¡u.
"""

import asyncio
import io
import json
import sys
from contextlib import asynccontextmanager, suppress

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

import dnse_stream
import homepage_service
import market_cache
import market_refresher
import sector_service
import signal_service
import tick_hub
import vn100_service
import vnstock_license


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Xác thực license vnstock trả phí (vnii) TRƯỚC khi warm/refresh — log tier
    # ngay đầu log khởi động. Lỗi/thiếu key không chặn app (rơi về Community).
    vnstock_license.ensure_license()
    # vnstock_data.core.utils.env.idv() (chạy lúc IMPORT vnstock_data) đòi
    # ~/.vnstock/user.json tồn tại với field "user" khác rỗng — file này bình
    # thường do vnstock_installer tạo lúc cài tương tác, container/CI không
    # có bước đó nên PHẢI tự tạo trước, nếu không import vnstock_data crash
    # (SystemExit "Không tìm thấy thông tin người dùng hợp lệ").
    vnstock_license.ensure_user_profile()
    # Giảm retry nội bộ (tenacity) của vnstock_data từ 3 xuống 1 TRƯỚC lần
    # import đầu tiên — sau đó set không còn tác dụng (decorator đã bake giá
    # trị gốc). Gọi lần 1 TRƯỚC ensure_vnstock_data(): bắt trường hợp gói ĐÃ
    # CÓ SẴN trên đĩa (vd máy dev — ensure_vnstock_data() bên dưới sẽ import
    # thành công ngay, quá muộn để patch nếu chờ tới đây). Lần 1 fail (gói
    # CHƯA cài, container mới, chưa có gì trên đĩa để tìm spec) → thử lại
    # NGAY SAU khi cài xong, vẫn kịp trước lần import thật sự đầu tiên
    # (data_source gọi sau, không phải ở đây).
    _patched_early = vnstock_license.patch_vnstock_data_retries()
    # vnstock_data không cài được qua pip (xem requirements.txt) — tự tải+cài
    # ở đây bằng API key vừa xác thực, TRƯỚC khi data_source gọi lần đầu.
    vnstock_license.ensure_vnstock_data()
    if not _patched_early:
        vnstock_license.patch_vnstock_data_retries()
    # Warm danh sÃƒÂ¡ch mÃƒÂ£ rÃ¡Â»â€¢ theo dÃƒÂµi + danh sÃƒÂ¡ch VN100 (cÃ¡Â»Â `vn100` trÃƒÂªn board)
    # lÃƒÂºc khÃ¡Â»Å¸i Ã„â€˜Ã¡Â»â„¢ng (memoize). LÃ¡Â»â€”i cÃ…Â©ng khÃƒÂ´ng sao Ã¢â‚¬â€ request /vn100 Ã„â€˜Ã¡ÂºÂ§u tiÃƒÂªn sÃ¡ÂºÂ½
    # tÃ¡Â»Â± thÃ¡Â»Â­ lÃ¡ÂºÂ¡i.
    market_refresher.load_board_vn100_cache()
    vn100_service.get_symbols()
    vn100_service.get_vn100_members()
    # NÃ¡ÂºÂ¡p cache tÃƒÂ­n hiÃ¡Â»â€¡u tÃ¡Â»Â« Ã„â€˜Ã„Â©a (phÃ¡Â»Â¥c vÃ¡Â»Â¥ ngay) + chÃ¡ÂºÂ¡y scheduler nÃ¡Â»Ân: warm nÃ¡ÂºÂ¿u
    # cache cÃ…Â© rÃ¡Â»â€œi refresh 1 lÃ¡ÂºÂ§n/ngÃƒÂ y sau Ã„â€˜ÃƒÂ³ng cÃ¡Â»Â­a. KhÃƒÂ´ng chÃ¡ÂºÂ·n server start.
    signal_service.load_cache()
    # NÃ¡ÂºÂ¡p nÃ¡ÂºÂ¿n base (_history_candles) tÃ¡Â»Â« Ã„â€˜Ã„Â©a Ã¢â‚¬â€ Ã„â€˜Ã¡Â»Æ’ restart giÃ¡Â»Â¯a phiÃƒÂªn (deploy/
    # crash/--reload) vÃ¡ÂºÂ«n tÃƒÂ­nh Ã„â€˜Ã†Â°Ã¡Â»Â£c tÃƒÂ­n hiÃ¡Â»â€¡u live theo giÃƒÂ¡ hiÃ¡Â»â€¡n tÃ¡ÂºÂ¡i ngay, thay
    # vÃƒÂ¬ Ã„â€˜ÃƒÂ³ng bÃ„Æ’ng Ã¡Â»Å¸ tÃƒÂ­n hiÃ¡Â»â€¡u cache cÃ…Â© tÃ¡Â»â€ºi tÃ¡ÂºÂ­n 15:05.
    signal_service.load_history_cache()
    asyncio.create_task(signal_service.scheduler_loop())
    # LuÃ¡Â»â€œng refresh nÃ¡Â»Ân cho snapshot thÃ¡Â»â€¹ trÃ†Â°Ã¡Â»Âng dÃƒÂ¹ng chung (board VN100 + toÃƒÂ n TT +
    # nÃ¡ÂºÂ¿n mÃ¡ÂºÂ·c Ã„â€˜Ã¡Â»â€¹nh). TÃ¡Â»Â± warm ngay khi khÃ¡Â»Å¸i Ã„â€˜Ã¡Â»â„¢ng Ã¢â€ â€™ trang cÃƒÂ³ nÃ¡Â»â„¢i dung dÃ¡Â»Â±ng sÃ¡ÂºÂµn.
    asyncio.create_task(market_refresher.scheduler_loop())
    # Nguồn realtime DNSE: 1 kết nối MQTT nền cho cả hệ thống, đẩy tick đã
    # normalize vào hub để /ws/quotes fan-out theo mã. Thiếu creds → bỏ qua
    # (log cảnh báo), FE tự fallback poll /quotes 5s — server vẫn sống.
    tick_hub.hub.set_loop(asyncio.get_running_loop())
    # SDK v2 là async → chạy như task trong loop (không block startup); connect
    # lỗi/thiếu creds tự degrade về poll bên trong start_stream.
    asyncio.create_task(dnse_stream.start_stream(tick_hub.hub.publish))
    yield
    await dnse_stream.stop_stream()


app = FastAPI(title="vnstock Realtime API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # restrict to your FE origin in production
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/api/python/vn100")
def get_vn100():
    """Ã†Â¯u tiÃƒÂªn view vn100 trong snapshot market_wide (dÃ¡Â»Â±ng lÃ¡ÂºÂ¡i mÃ¡Â»â€”i chu kÃ¡Â»Â³ ~20s
    tÃ¡Â»Â« board toÃƒÂ n TT Ã¢â€ â€™ signal cÃ¡ÂºÂ¯t trong phiÃƒÂªn hiÃ¡Â»â€¡n ngay); thiÃ¡ÂºÂ¿u (chÃ†Â°a warm /
    chÃ†Â°a memo rÃ¡Â»â€¢) Ã¢â€ â€™ snapshot board_vn100 (~1 tiÃ¡ÂºÂ¿ng) Ã¢â€ â€™ gÃ¡Â»Âi trÃ¡Â»Â±c tiÃ¡ÂºÂ¿p."""
    wide = market_cache.get_snapshot("market_wide")
    if wide and wide.get("vn100"):
        return wide["vn100"]
    snap = market_cache.get_snapshot("board_vn100")
    if snap:
        return snap["vn100"]
    return vn100_service.get_board()  # fallback: cache chÃ†Â°a warm / luÃ¡Â»â€œng nÃ¡Â»Ân chÃ¡ÂºÂ¿t


@app.get("/api/python/quotes")
def get_quotes():
    
    snap = market_cache.get_snapshot("quotes")
    if snap:
        return snap
    return market_refresher.fetch_quotes_direct()  # fallback: cache chÃ†Â°a warm


@app.get("/api/python/power")
def get_power():
    
    snap = market_cache.get_snapshot("market_wide")
    if snap and "power" in snap:
        return snap["power"]
    return vn100_service.get_power_board()  # fallback: cache chÃ†Â°a warm / luÃ¡Â»â€œng nÃ¡Â»Ân chÃ¡ÂºÂ¿t


@app.get("/api/python/intraday")
def get_intraday(
    symbol: str = Query(description="Stock ticker code, e.g. TCB, VNM, HPG"),
    interval: str = Query(
        "1d", description="Khung thÃ¡Â»Âi gian: 1m,5m,15m,30m,1h,1d,1w,1mth"
    ),
):
    # Cache theo (symbol, interval) + single-flight: N user mÃ¡Â»Å¸ cÃƒÂ¹ng mÃƒÂ£ = 1 call.
    result = market_cache.get_intraday(symbol, interval)
    return {"symbol": symbol, "interval": interval, **result}


@app.get("/api/python/sectors")
def get_sectors():
    snap = market_cache.get_snapshot("board_vn100")
    if snap:
        return snap["sectors"]
    return sector_service.get_sectors()


@app.get("/api/python/sectors/symbols")
def get_sector_symbols(
    icb_code: str = Query(description="ICB level-3 code, e.g. 8350 (NgÃƒÂ¢n hÃƒÂ ng)"),
):
    snap = market_cache.get_snapshot("board_vn100")
    if snap:
        return sector_service.sector_symbols_view(snap["groups"], icb_code)
    return sector_service.get_sector_symbols(icb_code)


@app.get("/api/python/heatmap")
def get_heatmap():
    
    snap = market_cache.get_snapshot("board_vn100")
    if snap:
        return snap["heatmap"]
    return sector_service.get_heatmap()


@app.get("/api/python/homepage/top-volume")
def get_homepage_top_volume(
    limit: int = Query(10, ge=1, le=100, description="SÃ¡Â»â€˜ mÃƒÂ£ top theo khÃ¡Â»â€˜i lÃ†Â°Ã¡Â»Â£ng, mÃ¡ÂºÂ·c Ã„â€˜Ã¡Â»â€¹nh 10"),
):
    """Top mÃƒÂ£ VN100 theo khÃ¡Â»â€˜i lÃ†Â°Ã¡Â»Â£ng phiÃƒÂªn gÃ¡ÂºÂ§n nhÃ¡ÂºÂ¥t + xu hÃ†Â°Ã¡Â»â€ºng mua/bÃƒÂ¡n (Ã„â€˜Ã¡Â»Âc cache)."""
    snap = market_cache.get_snapshot("market_wide")
    if snap and limit == 10:  # snapshot tÃƒÂ­nh sÃ¡ÂºÂµn cho limit mÃ¡ÂºÂ·c Ã„â€˜Ã¡Â»â€¹nh
        return snap["top_volume"]
    return homepage_service.get_top_volume(limit)


@app.get("/api/python/homepage/market-depth")
def get_homepage_market_depth():
    
    snap = market_cache.get_snapshot("market_wide")
    if snap:
        return snap["depth"]
    return homepage_service.get_market_depth()


@app.get("/api/python/homepage/market-breadth")
def get_homepage_market_breadth():

    snap = market_cache.get_snapshot("market_wide")
    if snap:
        return snap["breadth"]
    return homepage_service.get_market_breadth()


@app.websocket("/api/python/ws/quotes")
async def ws_quotes(websocket: WebSocket):
    """Stream quote realtime theo mã. Client gửi
    {"action":"subscribe"|"unsubscribe","symbol":"FPT"}; server đẩy MỖI quote
    MỘT message JSON {symbol, price, time, volume?}. Lệnh sai/JSON hỏng → bỏ
    qua, GIỮ kết nối. Coalesce ở tick_hub, nhịp đẩy tối đa FLUSH_INTERVAL_S.
    """
    await websocket.accept()
    # Đọc hub tại thời điểm gọi (test monkeypatch tick_hub.hub từng test một).
    hub = tick_hub.hub
    # TestClient không chạy lifespan → set loop "lười" ở đây; chạy thật thì
    # lifespan đã set cùng loop nên gọi lại vô hại (idempotent).
    hub.set_loop(asyncio.get_running_loop())
    sub = hub.connect()

    async def sender():
        while True:
            for quote in await hub.drain(sub):
                await websocket.send_json(quote)
            await asyncio.sleep(tick_hub.FLUSH_INTERVAL_S)

    send_task = asyncio.create_task(sender())
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(msg, dict):
                continue
            symbol = msg.get("symbol")
            if not isinstance(symbol, str) or not symbol:
                continue
            # want()/unwant() điều khiển subscribe SDK v2 theo nhu cầu (v2 không có
            # wildcard). Chỉ gọi khi mã THỰC SỰ đổi trạng thái với sub này để
            # refcount khớp với sub.symbols (subscribe/unsubscribe lặp không lệch).
            key = symbol.upper()
            if msg.get("action") == "subscribe":
                is_new = key not in sub.symbols
                hub.subscribe(sub, symbol)
                if is_new:
                    await dnse_stream.want(key)
            elif msg.get("action") == "unsubscribe":
                was_watching = key in sub.symbols
                hub.unsubscribe(sub, symbol)
                if was_watching:
                    await dnse_stream.unwant(key)
    except WebSocketDisconnect:
        pass
    finally:
        send_task.cancel()
        try:
            with suppress(asyncio.CancelledError):
                await send_task
        except Exception:
            # sender chết vì lỗi gửi (client rớt giữa chừng) cũng phải dọn sub,
            # không để Subscription mồ côi trong hub
            pass
        finally:
            # Rời mọi mã còn theo dõi để refcount SDK về 0 (unsubscribe khi hết xem).
            for watched in list(sub.symbols):
                await dnse_stream.unwant(watched)
            hub.disconnect(sub)

