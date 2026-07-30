"""Hai chart "GIÁ TRỊ TIỀN KHỚP LỆNH 5 PHIÊN GẦN NHẤT" và "TỶ TRỌNG ..." — cột
chá»“ng theo ngÃ nh, má»—i cá»™t lÃ  má»™t phiÃªn.

Cùng một payload phục vụ cả hai chart: chart trái đọc `value`, chart phải đọc
`pct`. Tá»± chuáº©n hÃ³a nÃªn tá»•ng má»—i phiÃªn luÃ´n Ä‘Ãºng 100%, khÃ´ng cáº§n cá»™t "Tá»•ng".

Ba điều quyết định tính đúng đắn của chart:

1. **5 phiÃªn GIAO Dá»ŠCH gáº§n nháº¥t, khÃ´ng pháº£i 5 ngÃ y lá»‹ch.** Láº¥y táº­p ngÃ y tháº­t cÃ³
   trong náº¿n rá»“i cáº¯t 5 ngÃ y cuá»‘i â€” `today - 5 days` sáº½ dÃ­nh cuá»‘i tuáº§n/nghá»‰ lá»… vÃ 
   ra cá»™t rá»—ng.

2. **Thứ tự ngành CỐ ĐỊNH và giống hệt nhau ở cả 5 cột lẫn cả 2 chart.** Xếp theo
   tá»•ng giÃ¡ trá»‹ cá»§a Cáº¢ 5 PHIÃŠN, khÃ´ng xáº¿p theo giÃ¡ trá»‹ tá»«ng ngÃ y â€” náº¿u má»—i cá»™t
   một thứ tự thì không đọc được xu hướng.

3. **NgÃ nh khÃ´ng giao dá»‹ch trong má»™t phiÃªn váº«n giá»¯ khÃºc giÃ¡ trá»‹ 0** (fillna(0)),
   náº¿u khÃ´ng thá»© tá»± stack lá»‡ch giá»¯a cÃ¡c cá»™t.

GiÃ¡ trá»‹ má»—i phiÃªn = volume Ã— close Ã— 1000 (close Ä‘Æ¡n vá»‹ nghÃ¬n Ä‘á»“ng â†’ VND), Ä‘Ãºng
công thức flow_surge_service dùng cho phiên nền. Đây là xấp xỉ giá trị khớp lệnh
â€” náº¿n ngÃ y khÃ´ng cÃ³ `accumulated_value` nhÆ° báº£ng giÃ¡ realtime.
"""

from app.services import sector_service

DEFAULT_SESSIONS = 5
UNCLASSIFIED = sector_service.UNCLASSIFIED

_NGHIN_TO_VND = 1000


def _candle_value_vnd(candle) -> float:
    value = candle.get("value")
    if value:
        return float(value)
    volume = candle.get("volume")
    close = candle.get("close")
    if not volume or not close:
        return 0.0
    return float(volume) * float(close) * _NGHIN_TO_VND


def latest_session_times(history_by_symbol, sessions=DEFAULT_SESSIONS) -> list[int]:
    """`sessions` mốc thời gian nến gần nhất có thật trong dữ liệu, xếp tăng dần.

    Gom tá»« Má»ŒI mÃ£: má»™t mÃ£ láº» cÃ³ thá»ƒ thiáº¿u phiÃªn (má»›i niÃªm yáº¿t, bá»‹ Ä‘Ã¬nh chá»‰), láº¥y
    theo má»™t mÃ£ thÃ¬ trá»¥c X sáº½ thiáº¿u cá»™t.
    """
    times = set()
    for candles in (history_by_symbol or {}).values():
        for candle in candles or []:
            time = candle.get("time")
            if time:
                times.add(int(time))
    return sorted(times)[-sessions:] if times else []


def build_sector_flow(
    history_by_symbol: dict,
    industry_map: dict | None = None,
    sessions: int = DEFAULT_SESSIONS,
) -> dict:
    """history_by_symbol: {mÃ£: [náº¿n ngÃ y cÃ³ time/close/volume]}.

    Tráº£ {sessions: [time...], industries: [{name, icb_code, values, pcts, total}]}
    â€” `values`/`pcts` cÃ¹ng Ä‘á»™ dÃ i vá»›i `sessions`, ngÃ nh xáº¿p theo `total` giáº£m dáº§n.
    """
    imap = industry_map or {}
    times = latest_session_times(history_by_symbol, sessions)
    if not times:
        return {"sessions": [], "industries": []}
    index_of = {time: i for i, time in enumerate(times)}

    groups: dict[str, dict] = {}
    for symbol, candles in (history_by_symbol or {}).items():
        info = imap.get(symbol) or {}
        icb_code = info.get("icb_code", "")
        key = icb_code or UNCLASSIFIED
        group = groups.get(key)
        if group is None:
            group = {
                "name": info.get("icb_name", UNCLASSIFIED),
                "icb_code": icb_code,
                # fillna(0) ngay tá»« Ä‘áº§u: ngÃ nh thiáº¿u phiÃªn váº«n Ä‘á»§ sá»‘ khÃºc.
                "values": [0.0] * len(times),
            }
            groups[key] = group
        for candle in candles or []:
            i = index_of.get(int(candle.get("time") or 0))
            if i is not None:
                group["values"][i] += _candle_value_vnd(candle)

    totals_per_session = [
        sum(g["values"][i] for g in groups.values()) for i in range(len(times))
    ]

    industries = []
    for group in groups.values():
        total = sum(group["values"])
        if total <= 0:
            continue
        industries.append(
            {
                **group,
                "total": total,
                "pcts": [
                    round(group["values"][i] / totals_per_session[i] * 100, 2)
                    if totals_per_session[i] > 0
                    else 0.0
                    for i in range(len(times))
                ],
            }
        )

    # Thá»© tá»± cá»‘ Ä‘á»‹nh cho cáº£ 5 cá»™t vÃ  cáº£ 2 chart: theo tá»•ng 5 phiÃªn.
    industries.sort(key=lambda g: g["total"], reverse=True)
    return {
        "sessions": times,
        "industries": industries,
        "totals": totals_per_session,
    }


def get_sector_flow(sessions: int = DEFAULT_SESSIONS) -> dict:
    """Điểm gọi runtime: đọc snapshot `sector_flow_history` (do market_refresher
    nạp 1 lần/ngày) — không gọi vnstock ở đường request."""
    from app.data import market_cache

    history = market_cache.get_snapshot("sector_flow_history") or {}
    return build_sector_flow(history, sector_service.get_industry_map(), sessions)

