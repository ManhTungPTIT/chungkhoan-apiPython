

import datetime as dt
import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)

VCI = "VCI"
# Giờ VN (UTC+7) — dùng khi cần "hôm nay" theo lịch giao dịch, không phụ thuộc
# múi giờ của máy chủ (đồng bộ với signal_service/market_refresher).
VN_TZ = dt.timezone(dt.timedelta(hours=7))


def _default_listing():
    from vnstock import Listing

    # Rổ theo dõi = VNALL (VNAllShare, 300 mã HOSE — HOSE không có "VN300"
    # chính thức) + toàn sàn HNX (~293 mã, để phủ các mã như MBS/IVS/API).
    # Dedupe giữ thứ tự. Một trong hai lời gọi lỗi → cả fetch lỗi (caller trả
    # None, thử lại sau) — tránh memoize rổ thiếu nửa sàn.
    listing = Listing()
    vnall = listing.symbols_by_group("VNALL").tolist()
    hnx = listing.symbols_by_group("HNX").tolist()
    return list(dict.fromkeys(vnall + hnx))


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
    """Lấy danh sách mã rổ VNALL + toàn sàn HNX (~593 mã). Trả None nếu lỗi
    (kể cả rate-limit).

    Tên hàm giữ `vn100` cho tương thích với callers/endpoint hiện có."""
    try:
        return listing_fn()
    except BaseException as e:  # noqa: BLE001 — cố ý bắt cả SystemExit
        logger.warning("listing VNALL+HNX thất bại: %s", _short(e))
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
    """Sổ lệnh mỗi mã (real-time, 1 request price_board): tổng dư mua/dư bán của 3
    bước giá + giá cao nhất mỗi bên.

    Trả [{symbol, bid_volume, ask_volume, max_bid_price, max_ask_price}]; None nếu
    lỗi; [] nếu rỗng. max_bid_price = bid_1 (giá mua cao nhất, mức giảm dần);
    max_ask_price = ask_3 (giá bán cao nhất, mức tăng dần)."""
    try:
        df = price_board_fn(symbols)
    except BaseException as e:  # noqa: BLE001 — cố ý bắt cả SystemExit
        logger.warning("price_board (bid/ask) thất bại: %s", _short(e))
        return None

    if df is None or getattr(df, "empty", False):
        return []
    return _map_bid_ask(df)


def fetch_market_snapshot(
    symbols: list[str],
    price_board_fn: Callable = _default_price_board,
) -> Optional[dict]:
    """1 request price_board → map ra CẢ board (match) lẫn sổ lệnh (bid/ask).

    Cùng một DataFrame price_board chứa đủ cột cho `_map_board` (giá/khối lượng/
    giá trị) và `_map_bid_ask` (dư mua/bán), nên gọi 1 lần rồi map 2 chiều thay vì
    2 request riêng — phục vụ market-breadth + market-depth + top-volume chung.

    Trả {"board": [...], "bid_ask": [...]}; None nếu lỗi (kể cả rate-limit);
    {"board": [], "bid_ask": []} nếu rỗng.
    """
    try:
        df = price_board_fn(symbols)
    except BaseException as e:  # noqa: BLE001 — cố ý bắt cả SystemExit
        logger.warning("price_board (snapshot) thất bại: %s", _short(e))
        return None

    if df is None or getattr(df, "empty", False):
        return {"board": [], "bid_ask": []}
    return {"board": _map_board(df), "bid_ask": _map_bid_ask(df)}


def _map_bid_ask(df) -> list[dict]:
    out = []
    for _, row in df.iterrows():
        out.append(
            {
                "symbol": row[("listing", "symbol")],
                "bid_volume": sum(_num(row[("bid_ask", f"bid_{i}_volume")]) for i in (1, 2, 3)),
                "ask_volume": sum(_num(row[("bid_ask", f"ask_{i}_volume")]) for i in (1, 2, 3)),
                "max_bid_price": _num(row[("bid_ask", "bid_1_price")]),  # giá mua cao nhất
                "max_ask_price": _num(row[("bid_ask", "ask_3_price")]),  # giá bán cao nhất
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


def fetch_prev_session_volume(
    symbol: str,
    history_fn: Callable = _default_history,
    today: Optional[dt.date] = None,
) -> Optional[int]:
    """KL khớp của phiên hoàn tất gần nhất TRƯỚC hôm nay (nến 1D) của 1 mã/index.

    Lấy ~10 ngày nến để chắc chắn vượt cuối tuần/nghỉ lễ, rồi chọn nến có ngày <
    hôm nay gần nhất. Trả None nếu fetch lỗi; 0 nếu không có phiên trước đó.

    "hôm nay" mặc định theo giờ VN (không dùng dt.date.today() của máy chủ) — nếu
    server chạy UTC, khoảng 00:00–07:00 giờ VN local date còn ở ngày hôm trước sẽ
    khiến chọn nhầm phiên (lùi thêm 1 ngày)."""
    today = today or dt.datetime.now(VN_TZ).date()
    start = (today - dt.timedelta(days=10)).isoformat()
    rows = fetch_intraday_history(symbol, start, today.isoformat(), "1D", history_fn)
    if rows is None:
        return None
    today_str = today.isoformat()
    prev = [r for r in rows if str(r.get("time", ""))[:10] < today_str]
    return _num(prev[-1].get("volume")) if prev else 0


def _map_board(df) -> list[dict]:
    out = []
    for _, row in df.iterrows():
        ref = row[("listing", "ref_price")] or 0
        price = row[("match", "match_price")]
        change_pct = round((price - ref) / ref * 100, 2) if ref else 0.0
        # accumulated_value của price_board tính bằng TRIỆU VND → quy về VND để
        # giữ nguyên đơn vị 'value' như API cũ (volume × close).
        value_millions = row[("match", "accumulated_value")]
        if not value_millions or value_millions != value_millions:  # None/0/NaN
            value_millions = 0
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
        if df is None or getattr(df, "empty", False):
            return {}
        return _map_industries(df)
    except BaseException as e:  # noqa: BLE001 — cố ý bắt cả SystemExit
        # Bao trùm cả _map_industries: bản vnstock đổi shape (đổi tên/thiếu cột) →
        # trả None như mọi fetch lỗi khác để KHÔNG làm chết market_refresher hay
        # ném 500 ở endpoint heatmap/sectors; get_industry_map sẽ thử lại sau.
        logger.warning("symbols_by_industries thất bại: %s", _short(e))
        return None


def _map_industries(df) -> dict:
    # icb_level có thể là số HOẶC chuỗi tùy phiên bản vnstock → ép về số trước khi
    # lọc cấp 3, tránh lọc rỗng âm thầm (mọi mã thành "Chưa phân loại").
    import pandas as pd

    level = pd.to_numeric(df["icb_level"], errors="coerce")
    lvl3 = df[level == 3]
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
