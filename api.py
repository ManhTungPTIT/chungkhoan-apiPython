"""FastAPI app — endpoint gọi vnstock trực tiếp (không cache).

Lúc khởi động warm danh sách mã VN100 (memoize). data_source vẫn bắt
BaseException nên endpoint không bao giờ trả 500 vì lỗi dữ liệu.
"""

import asyncio
import io
import sys
from contextlib import asynccontextmanager

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

import homepage_service
import intraday_service
import sector_service
import signal_service
import vn100_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Warm danh sách mã VN100 lúc khởi động (memoize). Lỗi cũng không sao —
    # request /vn100 đầu tiên sẽ tự thử lại.
    vn100_service.get_symbols()
    # Nạp cache tín hiệu từ đĩa (phục vụ ngay) + chạy scheduler nền: warm nếu
    # cache cũ rồi refresh 1 lần/ngày sau đóng cửa. Không chặn server start.
    signal_service.load_cache()
    asyncio.create_task(signal_service.scheduler_loop())
    yield


app = FastAPI(title="vnstock Realtime API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # restrict to your FE origin in production
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/api/python/vn100")
def get_vn100():
    return vn100_service.get_board()


@app.get("/api/python/intraday")
def get_intraday(
    symbol: str = Query(description="Stock ticker code, e.g. TCB, VNM, HPG"),
    interval: str = Query(
        "1d", description="Khung thời gian: 1m,5m,15m,30m,1h,1d,1w,1mth"
    ),
):
    result = intraday_service.get_intraday(symbol, interval)
    return {"symbol": symbol, "interval": interval, **result}


@app.get("/api/python/sectors")
def get_sectors():
    return sector_service.get_sectors()


@app.get("/api/python/sectors/symbols")
def get_sector_symbols(
    icb_code: str = Query(description="ICB level-3 code, e.g. 8350 (Ngân hàng)"),
):

    return sector_service.get_sector_symbols(icb_code)


@app.get("/api/python/heatmap")
def get_heatmap():
    """Treemap bản đồ nhiệt: [{ group, icb_code, symbols:[{symbol, change_pct, market_cap}] }]."""
    return sector_service.get_heatmap()


@app.get("/api/python/homepage/top-volume")
def get_homepage_top_volume(
    limit: int = Query(10, ge=1, le=100, description="Số mã top theo khối lượng, mặc định 10"),
):
    """Top mã VN100 theo khối lượng phiên gần nhất + xu hướng mua/bán (đọc cache)."""
    return homepage_service.get_top_volume(limit)


@app.get("/api/python/homepage/market-depth")
def get_homepage_market_depth():
    """Tổng cầu (chờ mua) / tổng cung (chờ bán) toàn thị trường — cộng 3+3 bước giá."""
    return homepage_service.get_market_depth()
