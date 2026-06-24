

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


def _default_all_listing():
    from vnstock import Listing

    return Listing(source=VCI).all_symbols()["symbol"].tolist()


def _default_history(symbol: str, start: str, end: str, interval: str = "1D"):
    from vnstock.api.quote import Quote

    return Quote(symbol=symbol, source=VCI).history(
        start=start, end=end, interval=interval
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


def fetch_all_symbols(
    listing_fn: Callable = _default_all_listing,
) -> Optional[list[str]]:
    """Toàn bộ mã đang niêm yết trên thị trường. Trả None nếu lỗi (kể cả rate-limit)."""
    try:
        return listing_fn()
    except BaseException as e:  # noqa: BLE001 — cố ý bắt cả SystemExit
        logger.warning("all_symbols thất bại: %s", _short(e))
        return None


def fetch_market_bid_ask(
    symbols: list[str],
    price_board_fn: Callable = _default_price_board,
) -> Optional[list[dict]]:
    """Khối lượng dư mua/dư bán mỗi mã = tổng 3 bước giá hiển thị (bid_1..3 / ask_1..3).

    Dữ liệu sổ lệnh real-time (1 request price_board). Trả [{symbol, bid_volume,
    ask_volume}]; None nếu lỗi; [] nếu rỗng."""
    try:
        df = price_board_fn(symbols)
    except BaseException as e:  # noqa: BLE001 — cố ý bắt cả SystemExit
        logger.warning("price_board (bid/ask) thất bại: %s", _short(e))
        return None

    if df is None or getattr(df, "empty", False):
        return []
    return _map_bid_ask(df)


def _map_bid_ask(df) -> list[dict]:
    out = []
    for _, row in df.iterrows():
        out.append(
            {
                "symbol": row[("listing", "symbol")],
                "bid_volume": sum(_num(row[("bid_ask", f"bid_{i}_volume")]) for i in (1, 2, 3)),
                "ask_volume": sum(_num(row[("bid_ask", f"ask_{i}_volume")]) for i in (1, 2, 3)),
            }
        )
    return out


def _num(x) -> int:
    """Ép về int; NaN/None/giá trị hỏng → 0 (mức giá trống của sổ lệnh)."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return 0
    return int(v) if v == v else 0  # v == v loại NaN


def fetch_intraday_history(
    symbol: str,
    start: str,
    end: str,
    interval: str = "1D",
    history_fn: Callable = _default_history,
) -> Optional[list[dict]]:
    """Lấy nến lịch sử 1 mã theo khung interval. Trả None nếu lỗi (kể cả rate-limit)."""
    try:
        df = history_fn(symbol, start, end, interval=interval)
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
    cols = [c for c in ["time", "open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[cols]
    # vnstock pad nến giờ nghỉ/lễ bằng NaN OHLC; bỏ để FE không nhận "nan"
    # (một nến NaN làm hỏng thang giá → chart trắng).
    ohlc = [c for c in ["open", "high", "low", "close"] if c in cols]
    if ohlc:
        df = df.dropna(subset=ohlc)
    return df.astype(str).to_dict(orient="records")


def _short(e: BaseException) -> str:
    return str(e).splitlines()[0][:120] if str(e) else type(e).__name__
