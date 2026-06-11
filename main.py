"""main.py — Công cụ xem bảng VN100 trên terminal (dùng cho kiểm tra tay).

Cách dùng:
    python main.py --interval 60

CLI này dùng lại cùng đường lấy dữ liệu với API (data_source + vn100_service),
tức gọi gộp `price_board` 1 request cho cả nhóm VN100 thay vì fan-out 100 request.
Tự làm mới theo chu kỳ cho đến khi nhấn Ctrl+C.
"""

import argparse
import datetime
import io
import os
import sys
import time

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import data_source
import vn100_service


def parse_args():
    parser = argparse.ArgumentParser(description="VN100 board viewer (terminal)")
    parser.add_argument(
        "--interval", type=int, default=60, help="Seconds between refreshes (default: 60)"
    )
    return parser.parse_args()


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
