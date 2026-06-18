

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


def _default_industries():
    from vnstock import Listing

    # Nguồn VCI cung cấp phân cấp ICB đầy đủ; nguồn mặc định (KBS) chỉ tới cấp 2.
    return Listing(source=VCI).symbols_by_industries()


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
                # OHLC phiên hôm nay — sẵn trong price_board, dùng cho /sectors.
                "open": row[("match", "open_price")],
                "high": row[("match", "highest")],
                "low": row[("match", "lowest")],
                "close": price,
            }
        )
    return out


def fetch_industry_map(
    industries_fn: Callable = _default_industries,
) -> Optional[dict]:
    """Bản đồ mã → nhóm ngành ICB cấp 3. Trả None nếu lỗi (kể cả rate-limit)."""
    try:
        df = industries_fn()
    except BaseException as e:  # noqa: BLE001 — cố ý bắt cả SystemExit
        logger.warning("symbols_by_industries thất bại: %s", _short(e))
        return None

    if df is None or getattr(df, "empty", False):
        return {}
    return _map_industries(df)


def _map_industries(df) -> dict:
    lvl3 = df[df["icb_level"] == 3]
    out = {}
    for _, row in lvl3.iterrows():
        out[row["symbol"]] = {
            "icb_code": str(row["icb_code"]),
            "icb_name": row["icb_name"],
        }
    return out


def _map_history(df) -> list[dict]:
    cols = [c for c in ["time", "open", "high", "low", "close"] if c in df.columns]
    df = df[cols].dropna(how="all")
    return df[cols].astype(str).to_dict(orient="records")


def _short(e: BaseException) -> str:
    return str(e).splitlines()[0][:120] if str(e) else type(e).__name__
