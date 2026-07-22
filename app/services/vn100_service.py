"""Phục vụ bảng giá rổ VNALL + toàn sàn HNX (~593 mã) — gọi vnstock trực tiếp,
không cache.

Tên module/endpoint giữ `vn100` cho tương thích (trước đây rổ VN100); nội dung
đã mở rộng lên VNALL + HNX. Danh sách mã thay đổi rất hiếm (~mỗi quý) nên được
memoize ở biến module-level: lấy 1 lần rồi giữ trong RAM. Mỗi lần lấy bảng giá
là 1 request `price_board` cho toàn bộ mã. Fetch hỏng (rate-limit/mạng) → trả
{data: []}.
"""

from datetime import datetime, timedelta, timezone

from app.data import data_source

VALUE_THRESHOLD = 1_000_000_000
VN_TZ = timezone(timedelta(hours=7))

_symbols: list[str] = []

_vn100_members: list[str] = []

_vn30_members: list[str] = []

# Carry-over cho /vn100 (build_board): đầu phiên hầu hết mã chưa kịp đạt
# VALUE_THRESHOLD của HÔM NAY nên danh sách rất ngắn dù tín hiệu vẫn tính được
# (attach_signals không phụ thuộc value). _prev_day_symbols giữ danh sách ĐÃ
# ĐẠT NGƯỠNG của phiên gần nhất, hợp cùng mã đạt ngưỡng hôm nay để hiển thị đủ
# ngay từ đầu phiên, tự thay bằng dữ liệu thật khi mã đó có giao dịch. Chỉ RAM,
# không cần sống sót qua restart.
_today_accum_symbols: set[str] = set()
_today_accum_date: str | None = None
_prev_day_symbols: set[str] = set()
SUPPORTED_EXCHANGES = ("HOSE", "HNX", "UPCOM")
def _normalize_symbols(symbols) -> list[str]:
    """Normalize the all-exchange symbol list and remove duplicates."""
    if not symbols:
        return []
    return list(dict.fromkeys(
        symbol.strip().upper()
        for symbol in symbols
        if isinstance(symbol, str) and symbol.strip()
    ))


def _process(board: list[dict]) -> list[dict]:
    """Lọc giá trị giao dịch > 1 tỷ, sắp theo value giảm dần."""
    filtered = [x for x in board if (x.get("value") or 0) > VALUE_THRESHOLD]
    return sorted(filtered, key=lambda x: x["value"] or 0, reverse=True)


def _process_with_carryover(board: list[dict], now=None) -> list[dict]:
    """Như _process nhưng hợp thêm danh sách đã đạt ngưỡng của phiên trước
    (_prev_day_symbols) — chỉ dùng cho build_board (/vn100), KHÔNG dùng cho
    build_power_board/get_active_symbols để không đổi hành vi các trang khác."""
    global _today_accum_symbols, _today_accum_date, _prev_day_symbols
    today = (now or datetime.now(VN_TZ)).strftime("%Y-%m-%d")
    if _today_accum_date != today:
        _prev_day_symbols = _today_accum_symbols
        _today_accum_symbols = set()
        _today_accum_date = today

    qualified_today = {
        x["symbol"] for x in board if (x.get("value") or 0) > VALUE_THRESHOLD
    }
    _today_accum_symbols |= qualified_today

    eligible = qualified_today | _prev_day_symbols
    filtered = [x for x in board if x["symbol"] in eligible]
    return sorted(filtered, key=lambda x: x["value"] or 0, reverse=True)


def get_symbols(fetch_fn=data_source.fetch_vn100_symbols) -> list[str]:
    """Lấy danh sách mã rổ VNALL + HNX (~593 mã) 1 lần rồi memoize.

    Fetch hỏng (trả None) → giữ rỗng để lần gọi sau thử lại (self-heal).
    """
    global _symbols
    if not _symbols:
        fetched = fetch_fn()
        if fetched:
            _symbols = _normalize_symbols(fetched)
    return _symbols


def get_vn100_members(fetch_fn=data_source.fetch_vn100_members) -> list[str]:
    """Danh sách 100 mã rổ VN100 — memoize 1 lần, phục vụ cờ `vn100` trên board
    (bản đồ sức mạnh dòng tiền chỉ hiển thị mã VN100).

    Fetch hỏng (trả None) → giữ rỗng để lần gọi sau thử lại (self-heal);
    khi rỗng, build_board gắn vn100=False toàn bộ và FE degrade mềm.
    """
    global _vn100_members
    if not _vn100_members:
        fetched = fetch_fn()
        if fetched:
            _vn100_members = fetched
    return _vn100_members


def get_vn30_members(fetch_fn=data_source.fetch_vn30_members) -> list[str]:
    """Danh sách 30 mã rổ VN30 — memoize 1 lần, phục vụ cộng value nhóm VN30 cho
    chart 'Chỉ số chung 3 sàn'. Fetch hỏng (None) → giữ rỗng, lần sau thử lại."""
    global _vn30_members
    if not _vn30_members:
        fetched = fetch_fn()
        if fetched:
            _vn30_members = fetched
    return _vn30_members


def get_active_symbols(fetch_fn=data_source.fetch_vn100_board) -> list[str]:
    """Mã trong rổ có value giao dịch > VALUE_THRESHOLD — cùng tập với bảng hiển thị.

    1 request price_board để lấy value rồi lọc + sort. Dùng cho signal_service:
    chỉ tính tín hiệu cho các mã đang giao dịch đủ lớn (né mã thanh khoản thấp).
    Không có mã nền / board fetch lỗi (None) → [] để lần sau thử lại (self-heal).
    """
    symbols = get_symbols()
    if not symbols:
        return []
    board = fetch_fn(symbols)
    if not board:
        return []
    return [x["symbol"] for x in _process(board)]


def build_board(board: list[dict], now=None) -> dict:
    """Dựng payload /vn100 từ board ĐÃ fetch sẵn: lọc value (kèm carry-over
    phiên trước, xem _process_with_carryover) + sort + gắn cờ VN100 + gắn tín hiệu.

    Tách khỏi get_board để market_refresher fetch board 1 lần rồi tái dùng cho
    nhiều view (vn100/sectors/heatmap) — không gọi price_board lại.

    Cờ `vn100` (bool) đánh dấu mã thuộc rổ VN100 — trang bản đồ sức mạnh dòng
    tiền lọc theo cờ này; chưa lấy được danh sách (rỗng) → False toàn bộ.

    Tín hiệu mua/bán (field `signal`) lấy từ cache signal_service (tính nền 1 lần/
    phiên) — import trễ để tránh vòng lặp import (signal_service cần vn100_service).
    """
    from app.services import signal_service

    members = set(get_vn100_members())
    rows = _process_with_carryover(board, now=now)
    for row in rows:
        row["vn100"] = row["symbol"] in members
    return {"data": signal_service.attach_signals(rows)}


def build_power_board(board: list[dict]) -> dict:
    """Dựng payload /power (bản đồ sức mạnh dòng tiền) từ board ĐÃ fetch sẵn:
    lọc value + sort (cùng _process với /vn100) rồi giữ lại mã thuộc rổ VN100,
    mỗi dòng chỉ 4 field symbol/price/change_pct/value — payload nhỏ, không
    kèm signal (trang power không dùng).

    Chưa lấy được danh sách VN100 (rỗng) → giữ nguyên board đã lọc value
    (degrade mềm, tự lành khi get_vn100_members lấy được ở chu kỳ sau).
    """
    members = set(get_vn100_members())
    rows = _process(board)
    if members:
        rows = [r for r in rows if r["symbol"] in members]
    return _power_payload(rows)


def _power_payload(rows: list[dict]) -> dict:
    """Chiếu rows về payload /power: mỗi dòng đúng 4 field."""
    return {
        "data": [
            {
                "symbol": r["symbol"],
                "price": r.get("price"),
                "change_pct": r.get("change_pct"),
                "value": r.get("value"),
            }
            for r in rows
        ]
    }


def power_board_from_vn100_rows(rows: list[dict]) -> dict:
    """Dựng payload /power từ rows của view /vn100 (snapshot board_vn100 —
    last-good phiên trước, đã lọc value + sort sẵn): lọc theo cờ `vn100` đã lưu
    thay vì gọi get_vn100_members (boot trước phiên có thể chưa lấy được rổ).
    Phiên trước members rỗng → mọi cờ False → giữ nguyên rows (degrade mềm,
    cùng tinh thần build_power_board khi members rỗng)."""
    flagged = [r for r in rows if r.get("vn100")]
    return _power_payload(flagged if flagged else rows)


def get_power_board(fetch_fn=data_source.fetch_vn100_board) -> dict:
    """Lấy payload /power trực tiếp (fallback khi cache market_wide trống):
    1 request price_board cho đúng rổ VN100 (~100 mã) → build_power_board.

    Members chưa lấy được / fetch lỗi → {data: []} (lần poll sau tự lành —
    lưu ý FE KHÔNG giữ data cũ: response rỗng-thành-công vẫn ghi đè cache
    react-query, trang power sẽ báo 'Không có dữ liệu')."""
    members = get_vn100_members()
    if not members:
        return {"data": []}
    board = fetch_fn(members)
    if board is None:
        return {"data": []}
    return build_power_board(board)

def get_foreign_trading_board(fetch_fn=data_source.fetch_foreign_board, top_n=20) -> dict:
    """Lấy payload /foreign-trading trực tiếp (fallback khi cache market_wide trống):
    1 request lấy giá trị mua/bán ròng khối ngoại cho rổ VN100 → build_foreign_trading_board.

    Members chưa lấy được / fetch lỗi → {buy: [], sell: []} (lần poll sau tự lành —
    FE KHÔNG giữ data cũ, response rỗng vẫn ghi đè cache react-query)."""
    members = get_vn100_members()
    if not members:
        return {"buy": [], "sell": []}
    board = fetch_fn(members)
    if board is None:
        return {"buy": [], "sell": []}
    return build_foreign_trading_board(board, top_n=top_n)


def build_foreign_trading_board(board, top_n=20) -> dict:
    """Tách board thành 2 nhóm: mua ròng (net_value > 0) / bán ròng (net_value < 0),
    mỗi nhóm lấy top_n mã theo |net_value| lớn nhất, sort giảm dần."""
    items = []
    for b in board:
        symbol = b.get("symbol")
        if not symbol:
            continue
        net_value = b.get("foreign_net_value")  # tỷ VND, dương = mua ròng, âm = bán ròng
        if net_value is None:
            continue
        items.append({
            "symbol": symbol,
            "price": b.get("price"),
            "change_pct": b.get("change_pct"),
            "net_value": net_value,
        })

    buys = sorted(
        (x for x in items if x["net_value"] > 0),
        key=lambda x: x["net_value"], reverse=True,
    )[:top_n]
    sells = sorted(
        (x for x in items if x["net_value"] < 0),
        key=lambda x: x["net_value"],  # âm nhất trước
    )[:top_n]

    return {"buy": buys, "sell": sells}


def get_board(fetch_fn=data_source.fetch_vn100_board) -> dict:
    """Lấy bảng VN100 trực tiếp: 1 request price_board → lọc + sort + gắn tín hiệu."""
    symbols = get_symbols()
    if not symbols:
        return {"data": []}
    board = fetch_fn(symbols)
    if board is None:
        return {"data": []}
    return build_board(board)
