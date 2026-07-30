"""Chart "NGÀNH HÚT TIỀN ĐỀU ĐẶN NHẤT 30 PHIÊN" — lưới nhiệt ngành × phiên.

Mỗi phiên, mỗi mã được chấm một điểm rời rạc theo CHIỀU GIÁ và ĐỘ MẠNH dòng
tiền; điểm ngành là trung bình có trọng số vốn hóa của các mã trong ngành; xếp
hạng ngành theo TB/ĐLC của chuỗi 30 điểm (cao = vừa mạnh vừa đều, không giật cục).

    Δ  = close(t) − close(t−1)
    val = close(t) × volume(t)
    TB  = trung bình val của `avg_window` phiên TRƯỚC t (không gồm t)

    Δ > 0  và val ≥ 1.5 × TB   → +100   (hút tiền mạnh)
    Δ > 0                       →  +50   (hút tiền)
    |Δ| / close(t−1) < 0.05%    →    0   (trung tính)
    Δ < 0                       →  −50   (thoát tiền)
    Δ < 0  và val ≥ 1.5 × TB   → −100   (thoát tiền mạnh)

    điểm_ngành(t) = Σ wᵢ × điểmᵢ(t),  wᵢ = vốn hóaᵢ / Σ vốn hóa ngành

Đọc CÙNG cache RAM như các chart khác (`signal_service._history_candles` +
snapshot board + bản đồ ngành memoize) → endpoint chỉ ĐỌC, không gọi vnstock.

Ba đánh đổi đã cân nhắc, ghi lại để sau khỏi "sửa nhầm cho đúng":

1. `val` = close × volume chứ không phải `accumulated_value` thật — nến lịch sử
   không lưu giá trị khớp lệnh. Sai số do giá khớp trung bình ≠ giá đóng cửa là
   NHẤT QUÁN qua các phiên, mà công thức chỉ dùng TỈ SỐ val/TB nên gần như
   không ảnh hưởng.

2. Trọng số vốn hóa là ẢNH CHỤP HÔM NAY áp cho cả 30 phiên (không có lịch sử
   `listed_share`). Tỷ trọng tương đối giữa các mã trong một ngành hiếm khi đảo
   trong 6 tuần; mã vừa phát hành thêm cổ phiếu sẽ lệch nhẹ.

3. Cửa sổ TB LOẠI chính phiên t. Để t trong cửa sổ thì phiên bùng nổ tự kéo mốc
   so sánh của mình lên và gần như không bao giờ đạt ngưỡng 1.5× — ngưỡng "mạnh"
   trở thành vô dụng.
"""

import statistics
from datetime import datetime

from app.services import signal_service

DEFAULT_SESSIONS = 30
AVG_WINDOW = 20
SURGE_RATIO = 1.5
FLAT_EPS_PCT = 0.05
# Cột "so với N phiên trước": so điểm MA hiện tại với MA của cửa sổ lùi N phiên.
# Vì vậy phải tính chuỗi điểm dài hơn `sessions` đúng N phiên.
MA_LOOKBACK = 5
# Cần tối thiểu ngần này phiên nền mới dám phán "mạnh/yếu"; ít hơn thì vẫn chấm
# ±50/0 theo chiều giá nhưng bỏ qua ngưỡng 1.5× (thà không phân loại còn hơn
# phân loại bằng trung bình của 2 phiên).
MIN_AVG_SAMPLES = 5
# Sàn ĐLC khi tính TB/ĐLC: ngành phẳng lì (ĐLC ~ 0) sẽ cho thương số vô hạn và
# chiếm hết top. Trên thang điểm ±100 thì 1.0 là "gần như không dao động".
MIN_STD = 1.0
# Ngành dưới ngần này mã không phải xu hướng ngành, chỉ là câu chuyện của 1-2 mã.
MIN_SYMBOLS = 3
# Ngành phải có điểm ở ít nhất ngần này phần chuỗi mới đủ để nói "đều đặn".
MIN_COVERAGE = 0.5


def _closed_sessions(candles, today):
    """[(ngày, close, val)] các phiên ĐÃ ĐÓNG, thứ tự thời gian tăng dần.

    Nến hôm nay bị loại: sau lần refresh 15:05 nó đã nằm trong _history_candles,
    giữ lại thì cột cuối lưới là phiên dở dang — cùng quy ước với top_gain_service.
    Nến hỏng time/giá → bỏ qua."""
    out = []
    for candle in candles or []:
        try:
            day = signal_service._candle_date(candle["time"])
        except (KeyError, TypeError, ValueError, OverflowError, OSError):
            continue
        if day >= today:
            continue
        close = candle.get("close") or 0
        volume = candle.get("volume") or 0
        if close <= 0:
            continue
        out.append((day, close, close * volume))
    out.sort(key=lambda x: x[0])
    return out


def score_symbol_sessions(candles, today, sessions=DEFAULT_SESSIONS, avg_window=AVG_WINDOW):
    """Điểm từng phiên của MỘT mã → {ngày: điểm} cho `sessions` phiên gần nhất.

    Trả rỗng nếu mã chưa đủ 2 nến (không tính được Δ)."""
    rows = _closed_sessions(candles, today)
    if len(rows) < 2:
        return {}

    values = [r[2] for r in rows]
    scores = {}
    # i bắt đầu từ 1: phiên đầu tiên không có phiên trước để so Δ.
    for i in range(max(1, len(rows) - sessions), len(rows)):
        day, close, _ = rows[i]
        prev_close = rows[i - 1][1]
        if prev_close <= 0:
            continue
        delta_pct = (close - prev_close) / prev_close * 100

        window = values[max(0, i - avg_window):i]
        avg = statistics.fmean(window) if len(window) >= MIN_AVG_SAMPLES else 0
        surge = avg > 0 and values[i] >= SURGE_RATIO * avg

        if abs(delta_pct) < FLAT_EPS_PCT:
            score = 0
        elif delta_pct > 0:
            score = 100 if surge else 50
        else:
            score = -100 if surge else -50
        scores[day] = score
    return scores


def _ma_shift(series_full, sessions, ma_lookback, mean_now):
    """{ma_prev, delta} — MA của cửa sổ LÙI `ma_lookback` phiên và mức thay đổi
    so với hiện tại.

    `delta` là CHÊNH LỆCH ĐIỂM, không phải phần trăm: điểm ngành là thang có dấu
    quanh 0 (±100), nên "% thay đổi" sẽ nổ khi mẫu số gần 0 và đổi dấu vô nghĩa
    khi MA vượt qua 0 (từ −2 lên +3 không phải "tăng 250%").

    Thiếu lịch sử để dựng đủ cửa sổ lùi → cả hai là None (FE hiện "—"), KHÔNG
    lấy cửa sổ ngắn hơn: so hai trung bình có số phiên khác nhau là so lệch."""
    if ma_lookback <= 0 or len(series_full) < sessions + ma_lookback:
        return {"ma_prev": None, "delta": None}
    prev_window = [s for s in series_full[:sessions] if s is not None]
    if len(prev_window) < sessions * MIN_COVERAGE:
        return {"ma_prev": None, "delta": None}
    ma_prev = statistics.fmean(prev_window)
    return {"ma_prev": round(ma_prev, 2), "delta": round(mean_now - ma_prev, 2)}


def caps_from_board(board_rows):
    """{mã: vốn hóa VND} = listed_share × giá.

    Mã thiếu `listed_share` (snapshot đĩa ghi trước khi data_source map field
    này, hoặc vendor trả rỗng) → lùi về `value` làm trọng số. Đây là DEGRADE
    MỀM có chủ ý: thanh khoản là proxy thô của quy mô, còn hơn gán trọng số 0
    làm mã đó biến mất khỏi ngành cho tới lần refresh kế."""
    caps = {}
    for row in board_rows or []:
        symbol = row.get("symbol")
        if not symbol:
            continue
        listed = row.get("listed_share") or 0
        price = row.get("price") or 0
        cap = listed * price
        if cap <= 0:
            cap = row.get("value") or 0
        if cap > 0:
            caps[symbol] = cap
    return caps


def compute_sector_flow_consistency(
    history_candles,
    caps,
    industry_map,
    sessions=DEFAULT_SESSIONS,
    avg_window=AVG_WINDOW,
    now=None,
    ma_lookback=MA_LOOKBACK,
):
    """Hàm thuần (dễ test). Trả {dates, sessions, ma_lookback, rows} — rows đã
    sort giảm dần theo `ratio` (TB ÷ ĐLC).

    `caps` là {mã: vốn hóa}; mã không có trong caps bị bỏ (không có trọng số thì
    không thể tham gia trung bình có trọng số)."""
    now = now or datetime.now(signal_service.VN_TZ)
    today = now.date().isoformat()
    # Tính DÀI HƠN `sessions` đúng `ma_lookback` phiên để còn dựng lại được MA
    # của cửa sổ lùi 5 phiên. Phần thừa CHỈ dùng cho cột so sánh, không lên lưới.
    span = sessions + max(ma_lookback, 0)

    per_symbol = {}
    for symbol, candles in (history_candles or {}).items():
        if symbol not in (caps or {}):
            continue
        scores = score_symbol_sessions(candles, today, span, avg_window)
        if scores:
            per_symbol[symbol] = scores

    # Trục ngày = HỢP các ngày có điểm rồi lấy `span` ngày cuối. Không lấy theo
    # một mã "chuẩn" nào cả: mã bị ngưng giao dịch vài phiên sẽ khuyết cột, và
    # trọng số ở cột đó tự chuẩn hóa lại theo các mã còn giao dịch.
    all_days = sorted({day for scores in per_symbol.values() for day in scores})
    dates_full = all_days[-span:]
    dates = dates_full[-sessions:]
    if not dates:
        return {"dates": [], "sessions": sessions, "ma_lookback": ma_lookback, "rows": []}

    # Gom mã theo ngành trước, để mỗi ngành chỉ duyệt các mã của mình.
    by_sector = {}
    for symbol in per_symbol:
        info = (industry_map or {}).get(symbol) or {}
        icb_code = info.get("icb_code") or ""
        if not icb_code:
            continue
        bucket = by_sector.setdefault(
            icb_code, {"group": info.get("icb_name") or "", "symbols": []}
        )
        bucket["symbols"].append(symbol)

    rows = []
    for icb_code, bucket in by_sector.items():
        symbols = bucket["symbols"]
        if len(symbols) < MIN_SYMBOLS:
            continue

        series_full = []
        for day in dates_full:
            weighted = 0.0
            total_cap = 0.0
            for symbol in symbols:
                score = per_symbol[symbol].get(day)
                if score is None:
                    continue
                cap = caps[symbol]
                weighted += cap * score
                total_cap += cap
            # Chuẩn hóa theo tập mã THỰC CÓ điểm ở phiên này → tổng trọng số
            # luôn bằng 1. Chia cho tổng vốn hóa cả ngành (kể cả mã khuyết) thì
            # phiên thiếu mã tự bị kéo về 0 và trông như "trung tính giả".
            series_full.append(round(weighted / total_cap, 2) if total_cap > 0 else None)

        series = series_full[-sessions:]
        scored = [s for s in series if s is not None]
        if len(scored) < len(dates) * MIN_COVERAGE:
            continue

        mean = statistics.fmean(scored)
        std = statistics.pstdev(scored) if len(scored) > 1 else 0.0
        rows.append(
            {
                "group": bucket["group"],
                "icb_code": icb_code,
                "symbol_count": len(symbols),
                "scores": series,
                # `mean` CHÍNH LÀ điểm dòng tiền MA(sessions) — cùng một số, giữ
                # một field để hai bên không thể lệch nhau.
                "mean": round(mean, 2),
                "std": round(std, 2),
                "ratio": round(mean / max(std, MIN_STD), 2),
                "positive_sessions": sum(1 for s in scored if s > 0),
                **_ma_shift(series_full, sessions, ma_lookback, mean),
            }
        )

    rows.sort(key=lambda r: r["ratio"], reverse=True)
    return {
        "dates": dates,
        "sessions": sessions,
        "ma_lookback": ma_lookback,
        "rows": rows,
    }


def get_sector_flow_consistency(sessions=DEFAULT_SESSIONS, avg_window=AVG_WINDOW):
    """Điểm gọi runtime: nến base RAM + snapshot board + bản đồ ngành memoize.
    Cache chưa warm → rows rỗng (chart hiện "chưa có dữ liệu", không lỗi)."""
    from app.services import sector_service, vn100_service

    history = getattr(signal_service, "_history_candles", None) or {}
    caps = caps_from_board(vn100_service._full_board())
    result = compute_sector_flow_consistency(
        history,
        caps,
        sector_service.get_industry_map(),
        sessions=sessions,
        avg_window=avg_window,
    )
    cache = getattr(signal_service, "_cache", None) or {}
    result["generated_at"] = cache.get("last_refresh")
    return result
