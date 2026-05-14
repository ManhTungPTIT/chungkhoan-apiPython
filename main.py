"""
main.py — Công cụ xem giá cổ phiếu intraday theo thời gian thực trên terminal.

Cách dùng:
    python main.py --symbol TCB --interval 60

Tham số dòng lệnh:
    --symbol    Mã cổ phiếu cần theo dõi (mặc định: TCB)
    --date      Ngày lấy dữ liệu, định dạng YYYY-MM-DD (mặc định: hôm nay)
    --interval  Số giây giữa mỗi lần làm mới dữ liệu (mặc định: 60)

Dữ liệu được lấy từ vnstock qua nguồn VCI, hiển thị dạng bảng trên terminal
và tự động làm mới theo chu kỳ cho đến khi người dùng nhấn Ctrl+C.
"""

import argparse
import datetime
import io
import os
import sys
import time

from vnstock.api.quote import Quote


def parse_args():
    """Đọc và trả về các tham số từ dòng lệnh."""
    parser = argparse.ArgumentParser(description="Real-time intraday stock price fetcher")
    parser.add_argument("--symbol", default="TCB", help="Stock ticker code (default: TCB)")
    parser.add_argument(
        "--date",
        default=str(datetime.date.today()),
        help="Date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Seconds between each refresh (default: 60)",
    )
    return parser.parse_args()


def fetch_intraday(symbol: str, date: str):
    """
    Lấy dữ liệu giao dịch intraday của mã cổ phiếu từ vnstock (nguồn VCI).

    Tham số:
        symbol  Mã cổ phiếu, ví dụ: "TCB", "VNM", "HPG"
        date    Ngày cần lấy (hiện tại vnstock tự xác định ngày theo phiên mới nhất)

    Trả về:
        DataFrame gồm các cột: time, price, volume, match_type, id
        Mỗi dòng là một lệnh khớp trong phiên giao dịch.
    """
    q = Quote(symbol=symbol, source="VCI")
    a = q.history(
        symbol=symbol,
        start="2025-01-01",
        end=date,
        resolution="1D",
        show_log=False,
    )
    # return q.intraday(symbol=symbol, show_log=False)
    return a.drop(columns=["volume"])


def display(symbol: str, interval: int, df, last_updated: str):
    """
    Xóa màn hình terminal và in lại bảng dữ liệu mới nhất.

    Tham số:
        symbol       Mã cổ phiếu (để hiển thị trên tiêu đề)
        interval     Chu kỳ làm mới (giây), chỉ dùng để hiển thị thông tin
        df           DataFrame dữ liệu intraday
        last_updated Thời điểm lấy dữ liệu gần nhất (chuỗi)
    """
    os.system("cls" if os.name == "nt" else "clear")
    print(f"[{symbol}] Last updated: {last_updated}  (refreshing every {interval}s)")
    print("-" * 60)
    print(df)
    print("\nPress Ctrl+C to stop.")


def main():
    """
    Vòng lặp chính: lấy dữ liệu → hiển thị → chờ → lặp lại.

    Luồng hoạt động:
        1. Đọc tham số dòng lệnh (symbol, date, interval)
        2. Gọi fetch_intraday() để lấy dữ liệu từ vnstock
        3. Gọi display() để in bảng lên terminal
        4. Chờ `interval` giây rồi quay lại bước 2
        5. Dừng khi người dùng nhấn Ctrl+C
    """
    args = parse_args()
    print(f"Starting real-time feed for {args.symbol} | date={args.date} | interval={args.interval}s")
    print("Fetching first batch...")

    while True:
        try:
            df = fetch_intraday(args.symbol, args.date)
            last_updated = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            display(args.symbol, args.interval, df, last_updated)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            # Nếu API lỗi (mất mạng, rate limit...) thì in thông báo và thử lại sau
            print(f"[ERROR] {e} — retrying in {args.interval}s")
        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
