"""Điểm DUY NHẤT chạm thư viện vnstock.

Mọi lời gọi vnstock được bọc `except BaseException` tại đây. Lý do: khi chạm
rate-limit, vnstock gọi sys.exit() → ném `SystemExit` (kế thừa BaseException,
KHÔNG phải Exception). Nếu chỉ bắt `Exception` thì SystemExit thoát ra và làm
endpoint trả 500. Bắt `BaseException` ở một nơi duy nhất giúp toàn hệ thống xuống
cấp êm: trả None để lớp trên giữ dữ liệu tốt gần nhất.

Hàm fetch nhận tham số `*_fn` cho phép tiêm nguồn dữ liệu giả khi kiểm thử
(không gọi vnstock thật).
"""

import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)

VCI = "VCI"


def _default_listing():
    from vnstock import Listing

    return Listing().symbols_by_group("VN100").tolist()


def _default_price_board(symbols: list[str]):
    from vnstock import Trading

    return Trading(symbol=symbols[0], source=VCI).price_board(symbols)


def _default_history(symbol: str, start: str, end: str):
    from vnstock.api.quote import Quote

    return Quote(symbol=symbol, source=VCI).history(
        start=start, end=end, interval="1D"
    )


def fetch_vn100_symbols(
    listing_fn: Callable = _default_listing,
) -> Optional[list[str]]:
    """Lấy danh sách mã nhóm VN100. Trả None nếu lỗi (kể cả rate-limit)."""
    try:
        return listing_fn()
    except BaseException as e:  # noqa: BLE001 — cố ý bắt cả SystemExit
        logger.warning("listing VN100 thất bại: %s", _short(e))
        return None


def fetch_vn100_board(
    symbols: list[str],
    price_board_fn: Callable = _default_price_board,
) -> Optional[list[dict]]:
    """Lấy snapshot nhiều mã trong 1 request. Trả None nếu lỗi (kể cả rate-limit)."""
    try:
        df = price_board_fn(symbols)
    except BaseException as e:  # noqa: BLE001 — cố ý bắt cả SystemExit
        logger.warning("price_board thất bại: %s", _short(e))
        return None

    if df is None or getattr(df, "empty", False):
        return []
    return _map_board(df)


def fetch_intraday_history(
    symbol: str,
    start: str,
    end: str,
    history_fn: Callable = _default_history,
) -> Optional[list[dict]]:
    """Lấy nến lịch sử 1 mã. Trả None nếu lỗi (kể cả rate-limit)."""
    try:
        df = history_fn(symbol, start, end)
    except BaseException as e:  # noqa: BLE001 — cố ý bắt cả SystemExit
        logger.warning("history(%s) thất bại: %s", symbol, _short(e))
        return None

    if df is None or getattr(df, "empty", False):
        return []
    return _map_history(df)


def _map_board(df) -> list[dict]:
    out = []
    for _, row in df.iterrows():
        ref = row[("listing", "ref_price")] or 0
        price = row[("match", "match_price")]
        change_pct = round((price - ref) / ref * 100, 2) if ref else 0.0
        # accumulated_value của price_board tính bằng TRIỆU VND → quy về VND để
        # giữ nguyên đơn vị 'value' như API cũ (volume × close).
        value_millions = row[("match", "accumulated_value")] or 0
        out.append(
            {
                "symbol": row[("listing", "symbol")],
                "price": price,
                "change_pct": change_pct,
                "value": value_millions * 1_000_000,
            }
        )
    return out


def _map_history(df) -> list[dict]:
    cols = [c for c in ["time", "open", "high", "low", "close"] if c in df.columns]
    df = df[cols].dropna(how="all")
    return df[cols].astype(str).to_dict(orient="records")


def _short(e: BaseException) -> str:
    return str(e).splitlines()[0][:120] if str(e) else type(e).__name__
