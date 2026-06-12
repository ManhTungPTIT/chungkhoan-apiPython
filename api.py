"""FastAPI app — endpoint gọi vnstock trực tiếp (không cache).

Lúc khởi động warm danh sách mã VN100 (memoize). data_source vẫn bắt
BaseException nên endpoint không bao giờ trả 500 vì lỗi dữ liệu.
"""

import io
import sys
from contextlib import asynccontextmanager

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

import intraday_service
import vn100_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Warm danh sách mã VN100 lúc khởi động (memoize). Lỗi cũng không sao —
    # request /vn100 đầu tiên sẽ tự thử lại.
    vn100_service.get_symbols()
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
):
    result = intraday_service.get_intraday(symbol)
    return {"symbol": symbol, **result}
