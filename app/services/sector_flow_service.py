"""Hai chart "GIÁ TRỊ TIỀN KHỚP LỆNH 5 PHIÊN GẦN NHẤT" và "TỶ TRỌNG ..." — cột
chồng theo ngành, mỗi cột là một phiên.

Cùng một payload phục vụ cả hai chart: chart trái đọc `value`, chart phải đọc
`pct`. Tự chuẩn hóa nên tổng mỗi phiên luôn đúng 100%, không cần cột "Tổng".

Ba điều quyết định tính đúng đắn của chart:

1. **5 phiên GIAO DỊCH gần nhất, không phải 5 ngày lịch.** Lấy tập ngày thật có
   trong nến rồi cắt 5 ngày cuối — `today - 5 days` sẽ dính cuối tuần/nghỉ lễ và
   ra cột rỗng.

2. **Thứ tự ngành CỐ ĐỊNH và giống hệt nhau ở cả 5 cột lẫn cả 2 chart.** Xếp theo
   tổng giá trị của CẢ 5 PHIÊN, không xếp theo giá trị từng ngày — nếu mỗi cột
   một thứ tự thì không đọc được xu hướng.

3. **Ngành không giao dịch trong một phiên vẫn giữ khúc giá trị 0** (fillna(0)),
   nếu không thứ tự stack lệch giữa các cột.

Giá trị mỗi phiên = volume × close × 1000 (close đơn vị nghìn đồng → VND), đúng
công thức flow_surge_service dùng cho phiên nền. Đây là xấp xỉ giá trị khớp lệnh
— nến ngày không có `accumulated_value` như bảng giá realtime.
"""

from app.services import sector_service

DEFAULT_SESSIONS = 5
UNCLASSIFIED = sector_service.UNCLASSIFIED

_NGHIN_TO_VND = 1000


def _candle_value_vnd(candle) -> float:
    volume = candle.get("volume")
    close = candle.get("close")
    if not volume or not close:
        return 0.0
    return float(volume) * float(close) * _NGHIN_TO_VND


def latest_session_times(history_by_symbol, sessions=DEFAULT_SESSIONS) -> list[int]:
    """`sessions` mốc thời gian nến gần nhất có thật trong dữ liệu, xếp tăng dần.

    Gom từ MỌI mã: một mã lẻ có thể thiếu phiên (mới niêm yết, bị đình chỉ), lấy
    theo một mã thì trục X sẽ thiếu cột.
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
    """history_by_symbol: {mã: [nến ngày có time/close/volume]}.

    Trả {sessions: [time...], industries: [{name, icb_code, values, pcts, total}]}
    — `values`/`pcts` cùng độ dài với `sessions`, ngành xếp theo `total` giảm dần.
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
                # fillna(0) ngay từ đầu: ngành thiếu phiên vẫn đủ số khúc.
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

    # Thứ tự cố định cho cả 5 cột và cả 2 chart: theo tổng 5 phiên.
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
