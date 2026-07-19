"""main.py — Công cụ xem bảng VN100 trên terminal (dùng cho kiểm tra tay).

Cách dùng:
    python main.py --interval 60

CLI này dùng lại cùng đường lấy dữ liệu với API (data_source + vn100_service),
tức gọi gộp `price_board` 1 request cho cả nhóm VN100 thay vì fan-out 100 request.
Tự làm mới theo chu kỳ cho đến khi nhấn Ctrl+C.

Mode thứ 2 — kiểm tra tick realtime qua WebSocket (không đi qua data_source,
gọi thẳng endpoint /api/python/ws/quotes như 1 client thật, server phải đang
chạy sẵn — vd `uvicorn api:app --port 8808`):
    python main.py --ws --symbols FPT,OIL
    python main.py --ws --symbols FPT --host ws://<production-host>/api/python/ws/quotes
"""

import argparse
import asyncio
import datetime
import io
import json
import os
import sys
import time

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import data_source
import vn100_service
import vnstock_license

DEFAULT_WS_HOST = "ws://127.0.0.1:8808/api/python/ws/quotes"


def parse_args():
    parser = argparse.ArgumentParser(description="VN100 board viewer (terminal)")
    parser.add_argument(
        "--interval", type=int, default=60, help="Seconds between refreshes (default: 60)"
    )
    parser.add_argument(
        "--ws", action="store_true", help="Mode kiểm tra tick realtime qua WebSocket thay vì bảng VN100"
    )
    parser.add_argument(
        "--symbols", default="", help="Danh sách mã cách nhau bằng dấu phẩy, vd FPT,OIL (dùng với --ws)"
    )
    parser.add_argument(
        "--host", default=DEFAULT_WS_HOST, help=f"WebSocket URL (mặc định {DEFAULT_WS_HOST})"
    )
    return parser.parse_args()


async def run_ws(host: str, symbols: list[str]):
    """Connect tới /ws/quotes như client thật, subscribe symbols, in mỗi tick nhận
    được ra terminal kèm giờ nhận (không phải giờ trong tick) để thấy độ trễ."""
    import websockets

    print(f"Ket noi {host} ...", flush=True)
    async with websockets.connect(host) as ws:
        for sym in symbols:
            await ws.send(json.dumps({"action": "subscribe", "symbol": sym}))
        print(f"Da subscribe: {symbols or '(khong co — se khong nhan tick nao)'}", flush=True)
        print("Cho tick... (Ctrl+C de dung)\n", flush=True)
        async for raw in ws:
            now = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"[{now}] {raw}", flush=True)


def fetch_board():
    """Lấy bảng VN100 đã lọc/sort, dùng lại logic của service. Trả None nếu lỗi."""
    symbols = data_source.fetch_vn100_symbols()
    if not symbols:
        return None
    board = data_source.fetch_vn100_board(symbols)
    if board is None:
        return None
    return vn100_service._process(board)


def display(interval: int, board, last_updated: str):
    os.system("cls" if os.name == "nt" else "clear")
    print(f"VN100 — Last updated: {last_updated}  (refreshing every {interval}s)")
    print("-" * 60)
    if not board:
        print("⚠  Chưa có dữ liệu — đang chờ API cập nhật...")
    else:
        print(f"{'Symbol':<8}{'Price':>12}{'Change%':>10}{'Value':>18}")
        for row in board:
            print(
                f"{row['symbol']:<8}{row['price']:>12}"
                f"{row['change_pct']:>10}{row['value']:>18,}"
            )
    print("\nPress Ctrl+C to stop.")


def main():
    args = parse_args()
    if args.ws:
        # Mode WS là client thuần (gọi qua HTTP/WS tới server đang chạy sẵn),
        # không đụng data_source/vnstock nên KHÔNG cần ensure_license/vnstock_data.
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
        asyncio.run(run_ws(args.host, symbols))
        return
    vnstock_license.ensure_license()
    vnstock_license.ensure_user_profile()
    # Giảm retry nội bộ vnstock_data TRƯỚC lần import đầu tiên — xem comment
    # đầy đủ trong api.py lifespan. Gọi lại sau ensure_vnstock_data() nếu lần
    # đầu fail (gói vừa được cài mới, chưa tồn tại trên đĩa lúc patch lần 1).
    _patched_early = vnstock_license.patch_vnstock_data_retries()
    vnstock_license.ensure_vnstock_data()
    if not _patched_early:
        vnstock_license.patch_vnstock_data_retries()
    print(f"Starting VN100 feed | interval={args.interval}s")
    print("Fetching first batch...")

    last_board = None
    while True:
        try:
            board = fetch_board()
            if board is not None:
                last_board = board
            last_updated = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            display(args.interval, last_board, last_updated)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"[ERROR] {e} — retrying in {args.interval}s")
        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
