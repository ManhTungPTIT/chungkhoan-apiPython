

import datetime as dt
import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)

VCI = "VCI"
# Giá» VN (UTC+7) â€” dÃ¹ng khi cáº§n "hÃ´m nay" theo lá»‹ch giao dá»‹ch, khÃ´ng phá»¥ thuá»™c
# mÃºi giá» cá»§a mÃ¡y chá»§ (Ä‘á»“ng bá»™ vá»›i signal_service/market_refresher).
VN_TZ = dt.timezone(dt.timedelta(hours=7))


def _default_listing():
    from vnstock_data import Listing

    # Rổ theo dõi = CỔ PHIẾU (type=="STOCK") trên cả 3 sàn HOSE + HNX + UPCOM,
    # để phủ cả mã thanh khoản cao trên UPCOM (VD: OIL). Dùng symbols_by_exchange
    # (KHÔNG dùng symbols_by_group("VNALL"/"HNX")) — group "VNALL"/"HNX" bị vỡ
    # qua vnstock_data: "HNX" bị _VCI_INDEX_MAPPING nuốt trước thành mã chỉ số
    # 'HNXIndex' (sai), "VNALL" map sang 'VNALLSHARE' nhưng backend VCI trả JSON
    # rỗng cho group này. Lọc type=="STOCK" để loại CW/ETF/FU/UNIT_TRUST — các
    # loại này cũng mang exchange HSX/HNX/UPCOM nên lọt qua nếu chỉ lọc exchange
    # (bug đã gặp: mã "41I1G7000" là CW lẫn vào, không phải cổ phiếu).
    df = Listing(source=VCI).symbols_by_exchange()
    mask = df["exchange"].isin(["HSX", "HNX", "UPCOM"]) & (df["type"] == "STOCK")
    return df.loc[mask, "symbol"].tolist()


def _default_price_board(symbols: list[str]):
    from vnstock_data import Trading

    return Trading(symbol=symbols[0], source=VCI).price_board(symbols)


def _default_all_listing():
    from vnstock_data import Listing

    return Listing(source=VCI).all_symbols()["symbol"].tolist()


def _default_history(symbol: str, start: str, end: str, interval: str = "1D"):
    from vnstock_data import Quote

    return Quote(symbol=symbol, source=VCI).history(
        start=start, end=end, interval=interval
    )


def _default_industries():
    from vnstock_data import Listing

    # Nguá»“n VCI cung cáº¥p phÃ¢n cáº¥p ICB Ä‘áº§y Ä‘á»§; nguá»“n máº·c Ä‘á»‹nh (KBS) chá»‰ tá»›i cáº¥p 2.
    return Listing(source=VCI).symbols_by_industries()


def fetch_vn100_symbols(
    listing_fn: Callable = _default_listing,
) -> Optional[list[str]]:
    """Láº¥y danh sÃ¡ch mÃ£ rá»• VNALL + toÃ n sÃ n HNX (~593 mÃ£). Tráº£ None náº¿u lá»—i
    (ká»ƒ cáº£ rate-limit).

    TÃªn hÃ m giá»¯ `vn100` cho tÆ°Æ¡ng thÃ­ch vá»›i callers/endpoint hiá»‡n cÃ³."""
    try:
        return listing_fn()
    except BaseException as e:  # noqa: BLE001 â€” cá»‘ Ã½ báº¯t cáº£ SystemExit
        logger.warning("listing VNALL+HNX tháº¥t báº¡i: %s", _short(e))
        return None


def _default_vn100_members_listing():
    from vnstock_data import Listing

    return Listing(source=VCI).symbols_by_group("VN100").tolist()


def fetch_vn100_members(
    listing_fn: Callable = _default_vn100_members_listing,
) -> Optional[list[str]]:
    """Danh sÃ¡ch 100 mÃ£ rá»• VN100 â€” phá»¥c vá»¥ cá» `vn100` trÃªn board (báº£n Ä‘á»“ sá»©c
    máº¡nh dÃ²ng tiá»n chá»‰ hiá»ƒn thá»‹ mÃ£ VN100). Tráº£ None náº¿u lá»—i (ká»ƒ cáº£ rate-limit).

    TÃªn `members` (khÃ´ng pháº£i `symbols`) vÃ¬ `fetch_vn100_symbols` Ä‘Ã£ bá»‹ chiáº¿m
    cho rá»• theo dÃµi má»Ÿ rá»™ng VNALL + HNX."""
    try:
        return listing_fn()
    except BaseException as e:  # noqa: BLE001 â€” cá»‘ Ã½ báº¯t cáº£ SystemExit
        logger.warning("listing VN100 tháº¥t báº¡i: %s", _short(e))
        return None


def fetch_vn100_board(
    symbols: list[str],
    price_board_fn: Callable = _default_price_board,
) -> Optional[list[dict]]:
    """Láº¥y snapshot nhiá»u mÃ£ trong 1 request. Tráº£ None náº¿u lá»—i (ká»ƒ cáº£ rate-limit)."""
    try:
        df = price_board_fn(symbols)
    except BaseException as e:  # noqa: BLE001 â€” cá»‘ Ã½ báº¯t cáº£ SystemExit
        logger.warning("price_board tháº¥t báº¡i: %s", _short(e))
        return None

    if df is None or getattr(df, "empty", False):
        return []
    try:
        return _map_board(df)
    except BaseException as e:  # noqa: BLE001 - map shape/data errors are fetch failures
        logger.warning("price_board map that bai: %s", _short(e), exc_info=True)
        return None


def fetch_all_symbols(
    listing_fn: Callable = _default_all_listing,
) -> Optional[list[str]]:
    """ToÃ n bá»™ mÃ£ Ä‘ang niÃªm yáº¿t trÃªn thá»‹ trÆ°á»ng. Tráº£ None náº¿u lá»—i (ká»ƒ cáº£ rate-limit)."""
    try:
        return listing_fn()
    except BaseException as e:  # noqa: BLE001 â€” cá»‘ Ã½ báº¯t cáº£ SystemExit
        logger.warning("all_symbols tháº¥t báº¡i: %s", _short(e))
        return None


def fetch_market_bid_ask(
    symbols: list[str],
    price_board_fn: Callable = _default_price_board,
) -> Optional[list[dict]]:
    """Sá»• lá»‡nh má»—i mÃ£ (real-time, 1 request price_board): tá»•ng dÆ° mua/dÆ° bÃ¡n cá»§a 3
    bÆ°á»›c giÃ¡ + giÃ¡ cao nháº¥t má»—i bÃªn.

    Tráº£ [{symbol, bid_volume, ask_volume, max_bid_price, max_ask_price}]; None náº¿u
    lá»—i; [] náº¿u rá»—ng. max_bid_price = bid_1 (giÃ¡ mua cao nháº¥t, má»©c giáº£m dáº§n);
    max_ask_price = ask_3 (giÃ¡ bÃ¡n cao nháº¥t, má»©c tÄƒng dáº§n)."""
    try:
        df = price_board_fn(symbols)
    except BaseException as e:  # noqa: BLE001 â€” cá»‘ Ã½ báº¯t cáº£ SystemExit
        logger.warning("price_board (bid/ask) tháº¥t báº¡i: %s", _short(e))
        return None

    if df is None or getattr(df, "empty", False):
        return []
    try:
        return _map_bid_ask(df)
    except BaseException as e:  # noqa: BLE001 - map shape/data errors are fetch failures
        logger.warning("price_board map bid/ask that bai: %s", _short(e), exc_info=True)
        return None


def fetch_market_snapshot(
    symbols: list[str],
    price_board_fn: Callable = _default_price_board,
) -> Optional[dict]:
    """1 request price_board â†’ map ra Cáº¢ board (match) láº«n sá»• lá»‡nh (bid/ask).

    CÃ¹ng má»™t DataFrame price_board chá»©a Ä‘á»§ cá»™t cho `_map_board` (giÃ¡/khá»‘i lÆ°á»£ng/
    giÃ¡ trá»‹) vÃ  `_map_bid_ask` (dÆ° mua/bÃ¡n), nÃªn gá»i 1 láº§n rá»“i map 2 chiá»u thay vÃ¬
    2 request riÃªng â€” phá»¥c vá»¥ market-breadth + market-depth + top-volume chung.

    Tráº£ {"board": [...], "bid_ask": [...]}; None náº¿u lá»—i (ká»ƒ cáº£ rate-limit);
    {"board": [], "bid_ask": []} náº¿u rá»—ng.
    """
    try:
        df = price_board_fn(symbols)
    except BaseException as e:  # noqa: BLE001 â€” cá»‘ Ã½ báº¯t cáº£ SystemExit
        logger.warning("price_board (snapshot) tháº¥t báº¡i: %s", _short(e))
        return None

    if df is None or getattr(df, "empty", False):
        return {"board": [], "bid_ask": []}
    try:
        return {"board": _map_board(df), "bid_ask": _map_bid_ask(df)}
    except BaseException as e:  # noqa: BLE001 - map shape/data errors are fetch failures
        logger.warning("price_board map snapshot that bai: %s", _short(e), exc_info=True)
        return None


def _map_bid_ask(df) -> list[dict]:
    out = []
    for _, row in df.iterrows():
        out.append(
            {
                "symbol": row[("listing", "symbol")],
                "bid_volume": sum(_num(row[("bid_ask", f"bid_{i}_volume")]) for i in (1, 2, 3)),
                "ask_volume": sum(_num(row[("bid_ask", f"ask_{i}_volume")]) for i in (1, 2, 3)),
                "max_bid_price": _num(row[("bid_ask", "bid_1_price")]),  # giÃ¡ mua cao nháº¥t
                "max_ask_price": _num(row[("bid_ask", "ask_3_price")]),  # giÃ¡ bÃ¡n cao nháº¥t
            }
        )
    return out


def _num(x) -> int:
    """Ã‰p vá» int; NaN/None/giÃ¡ trá»‹ há»ng â†’ 0 (má»©c giÃ¡ trá»‘ng cá»§a sá»• lá»‡nh)."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return 0
    return int(v) if v == v else 0  # v == v loáº¡i NaN


def fetch_intraday_history(
    symbol: str,
    start: str,
    end: str,
    interval: str = "1D",
    history_fn: Callable = _default_history,
) -> Optional[list[dict]]:
    """Láº¥y náº¿n lá»‹ch sá»­ 1 mÃ£ theo khung interval. Tráº£ None náº¿u lá»—i (ká»ƒ cáº£ rate-limit)."""
    try:
        df = history_fn(symbol, start, end, interval=interval)
    except BaseException as e:  # noqa: BLE001 â€” cá»‘ Ã½ báº¯t cáº£ SystemExit
        logger.warning("history(%s) tháº¥t báº¡i: %s", symbol, _short(e))
        return None

    if df is None or getattr(df, "empty", False):
        return []
    return _map_history(df)


def fetch_prev_session_volume(
    symbol: str,
    history_fn: Callable = _default_history,
    today: Optional[dt.date] = None,
) -> Optional[int]:
    """KL khá»›p cá»§a phiÃªn hoÃ n táº¥t gáº§n nháº¥t TRÆ¯á»šC hÃ´m nay (náº¿n 1D) cá»§a 1 mÃ£/index.

    Láº¥y ~10 ngÃ y náº¿n Ä‘á»ƒ cháº¯c cháº¯n vÆ°á»£t cuá»‘i tuáº§n/nghá»‰ lá»…, rá»“i chá»n náº¿n cÃ³ ngÃ y <
    hÃ´m nay gáº§n nháº¥t. Tráº£ None náº¿u fetch lá»—i; 0 náº¿u khÃ´ng cÃ³ phiÃªn trÆ°á»›c Ä‘Ã³.

    "hÃ´m nay" máº·c Ä‘á»‹nh theo giá» VN (khÃ´ng dÃ¹ng dt.date.today() cá»§a mÃ¡y chá»§) â€” náº¿u
    server cháº¡y UTC, khoáº£ng 00:00â€“07:00 giá» VN local date cÃ²n á»Ÿ ngÃ y hÃ´m trÆ°á»›c sáº½
    khiáº¿n chá»n nháº§m phiÃªn (lÃ¹i thÃªm 1 ngÃ y)."""
    today = today or dt.datetime.now(VN_TZ).date()
    start = (today - dt.timedelta(days=10)).isoformat()
    rows = fetch_intraday_history(symbol, start, today.isoformat(), "1D", history_fn)
    if rows is None:
        return None
    today_str = today.isoformat()
    prev = [r for r in rows if _history_time_date(r.get("time")) < today_str]
    return _num(prev[-1].get("volume")) if prev else 0


def _map_board(df) -> list[dict]:
    out = []
    for _, row in df.iterrows():
        # Bá» dÃ²ng khÃ´ng cÃ³ symbol (NaN â€” mÃ£ thiáº¿u listingInfo, gáº·p á»Ÿ board toÃ n
        # TT): dÃ²ng rÃ¡c vÃ´ nghÄ©a vá»›i má»i view, vÃ  key NaN trong dict /quotes
        # khiáº¿n json.dumps (allow_nan=False) 500 cáº£ endpoint.
        symbol = row[("listing", "symbol")]
        if not isinstance(symbol, str) or not symbol:
            continue
        # MÃ£ chÆ°a khá»›p lá»‡nh (nháº¥t lÃ  mÃ£ HNX/HOSE thanh khoáº£n tháº¥p) tráº£ OHLC = NaN.
        # Ã‰p qua _num (NaN/None â†’ 0) Ä‘á»ƒ KHÃ”NG lá»t NaN ra JSON: Starlette serialize
        # vá»›i allow_nan=False, má»™t NaN lÃ  500 cáº£ endpoint /vn100.
        ref = _num(row[("listing", "ref_price")])
        price = _num(row[("match", "match_price")])
        # price=0 (chÆ°a khá»›p) â†’ coi nhÆ° Ä‘á»©ng giÃ¡ tham chiáº¿u (0%), trÃ¡nh -100% áº£o.
        change_pct = round((price - ref) / ref * 100, 2) if ref and price else 0.0
        # accumulated_value cá»§a price_board tÃ­nh báº±ng TRIá»†U VND â†’ quy vá» VND Ä‘á»ƒ
        # giá»¯ nguyÃªn Ä‘Æ¡n vá»‹ 'value' nhÆ° API cÅ© (volume Ã— close).
        value_millions = row[("match", "accumulated_value")]
        if not value_millions or value_millions != value_millions:  # None/0/NaN
            value_millions = 0
        out.append(
            {
                "symbol": symbol,
                "price": price,
                "change_pct": change_pct,
                "value": value_millions * 1_000_000,
                # KL khá»›p tÃ­ch lÅ©y phiÃªn (cá»• phiáº¿u) â€” cho snapshot /quotes.
                "volume": _num(row[("match", "accumulated_volume")]),
                # OHLC phiÃªn hÃ´m nay â€” sáºµn trong price_board, dÃ¹ng cho /sectors.
                "open": _num(row[("match", "open_price")]),
                "high": _num(row[("match", "highest")]),
                "low": _num(row[("match", "lowest")]),
                "close": price,
            }
        )
    return out


def fetch_industry_map(
    industries_fn: Callable = _default_industries,
) -> Optional[dict]:
    """Báº£n Ä‘á»“ mÃ£ â†’ nhÃ³m ngÃ nh ICB cáº¥p 3. Tráº£ None náº¿u lá»—i (ká»ƒ cáº£ rate-limit)."""
    try:
        df = industries_fn()
        if df is None or getattr(df, "empty", False):
            return {}
        return _map_industries(df)
    except BaseException as e:  # noqa: BLE001 â€” cá»‘ Ã½ báº¯t cáº£ SystemExit
        # Bao trÃ¹m cáº£ _map_industries: báº£n vnstock Ä‘á»•i shape (Ä‘á»•i tÃªn/thiáº¿u cá»™t) â†’
        # tráº£ None nhÆ° má»i fetch lá»—i khÃ¡c Ä‘á»ƒ KHÃ”NG lÃ m cháº¿t market_refresher hay
        # nÃ©m 500 á»Ÿ endpoint heatmap/sectors; get_industry_map sáº½ thá»­ láº¡i sau.
        logger.warning("symbols_by_industries tháº¥t báº¡i: %s", _short(e))
        return None


def _map_industries(df) -> dict:
    # icb_level cÃ³ thá»ƒ lÃ  sá»‘ HOáº¶C chuá»—i tÃ¹y phiÃªn báº£n vnstock â†’ Ã©p vá» sá»‘ trÆ°á»›c khi
    # lá»c cáº¥p 3, trÃ¡nh lá»c rá»—ng Ã¢m tháº§m (má»i mÃ£ thÃ nh "ChÆ°a phÃ¢n loáº¡i").
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


def _history_time_to_unix_seconds(value) -> Optional[int]:
    import pandas as pd

    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        value = float(value)
        if value > 10_000_000_000:  # milliseconds -> seconds
            value /= 1000
        return int(value)

    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize(VN_TZ)
    else:
        ts = ts.tz_convert(VN_TZ)
    return int(ts.timestamp())


def _history_time_date(value) -> str:
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value, VN_TZ).date().isoformat()
    return str(value or "")[:10]


def _map_history(df) -> list[dict]:
    aliases = {
        "date": "time",
        "trading_date": "time",
        "tradingDate": "time",
        "datetime": "time",
        "timestamp": "time",
        "open_price": "open",
        "openPrice": "open",
        "highest": "high",
        "high_price": "high",
        "highPrice": "high",
        "lowest": "low",
        "low_price": "low",
        "lowPrice": "low",
        "close_price": "close",
        "closePrice": "close",
        "match_price": "close",
        "matchPrice": "close",
        "match_volume": "volume",
        "matchVolume": "volume",
        "accumulated_volume": "volume",
        "accumulatedVolume": "volume",
    }
    df = df.rename(columns={k: v for k, v in aliases.items() if k in df.columns and v not in df.columns})
    cols = [c for c in ["time", "open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[cols]
    # vnstock pad nến giờ nghỉ/lễ bằng NaN OHLC; bỏ để FE không nhận "nan"
    # (một nến NaN làm hỏng thang giá -> chart trắng).
    ohlc = [c for c in ["open", "high", "low", "close"] if c in cols]
    if ohlc:
        df = df.dropna(subset=ohlc)
    rows = df.astype(str).to_dict(orient="records")
    for row in rows:
        if "time" in row:
            row["time"] = _history_time_to_unix_seconds(row["time"])
    return rows

def _short(e: BaseException) -> str:
    return str(e).splitlines()[0][:120] if str(e) else type(e).__name__
