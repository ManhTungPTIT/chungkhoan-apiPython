import datetime
import io
import sys

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from main import fetch_intraday

app = FastAPI(title="vnstock Realtime API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # restrict to your FE origin in production
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/intraday")
def get_intraday(
    symbol: str = Query(description="Stock ticker code, e.g. TCB, VNM, HPG"),
    date: str = Query(default=str(datetime.date.today()), description="Date YYYY-MM-DD"),
):
    try:
        df = fetch_intraday(symbol=symbol, date=date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    
    if df is None or df.empty:
        return {"symbol": symbol, "date": date, "data": []}

    cols = [c for c in ["time", "open", "high", "low", "close"] if c in df.columns]
    records = df[cols].astype(str).to_dict(orient="records")
    return {"symbol": symbol, "date": date, "data": records}
