# Tách price_board theo sàn (HSX/HNX/UPCOM) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `data_source._default_price_board()` chia list mã đầu vào thành các nhóm theo sàn (HSX/HNX/UPCOM) và gọi `Trading(...).price_board()` riêng cho từng nhóm thay vì 1 request gộp toàn bộ (~1600 mã), rồi gộp kết quả lại — tránh timeout/lỗi payload lớn.

**Architecture:** Thay đổi khoanh vùng hoàn toàn trong `_default_price_board()` — hàm này là default value của tham số `price_board_fn` dùng chung bởi `fetch_vn100_board`, `fetch_market_bid_ask`, `fetch_market_snapshot`, nên không cần đổi bất kỳ call site nào ở `market_refresher.py`. Cần thêm 1 bản đồ `symbol -> exchange` (memoize theo ngày, nạp từ `Listing(source=VCI).symbols_by_exchange()`) để biết chia nhóm thế nào.

**Tech Stack:** Python, pandas (`pd.concat` để gộp DataFrame), pytest (test injection qua tham số `Callable`, theo pattern `_default_*` đã có trong `data_source.py`).

## Global Constraints

- KHÔNG đổi signature của `fetch_vn100_board`, `fetch_market_bid_ask`, `fetch_market_snapshot`, `fetch_quotes_direct` hay bất kỳ call site nào ở `market_refresher.py`.
- Test hiện tại inject `price_board_fn` trực tiếp (bypass `_default_price_board`) — không được sửa các test này.
- Lỗi 1 sàn KHÔNG được làm fail toàn bộ: gộp kết quả từng phần, chỉ raise khi TẤT CẢ nhóm đều lỗi (giữ hợp đồng "fetch lỗi → None" hiện có ở các hàm `fetch_*` bao ngoài).
- Mã không tra được sàn (không có trong bản đồ) vẫn phải được gửi request riêng (nhóm `"unknown"`), không được rớt khỏi kết quả.
- Theo spec: `docs/superpowers/specs/2026-07-19-price-board-split-by-exchange-design.md`.

---

### Task 1: Bản đồ symbol → exchange, memoize theo ngày

**Files:**
- Modify: `data_source.py:31` (chèn TRƯỚC định nghĩa `_default_price_board` hiện tại)
- Test: `test/test_data_source.py` (thêm cuối file)

**Interfaces:**
- Produces: `_default_exchange_listing() -> DataFrame` (cột `symbol`, `exchange`), `_symbol_exchange_map(listing_fn: Callable = _default_exchange_listing) -> dict[str, str]`, module-level state `_exchange_map: dict[str, str]`, `_exchange_map_date: str | None`.

- [ ] **Step 1: Viết test cho `_symbol_exchange_map`**

Thêm vào cuối `test/test_data_source.py`:

```python
def _reset_exchange_memo(monkeypatch):
    monkeypatch.setattr(data_source, "_exchange_map", {})
    monkeypatch.setattr(data_source, "_exchange_map_date", None)


def test_symbol_exchange_map_builds_from_listing(monkeypatch):
    _reset_exchange_memo(monkeypatch)
    df = pd.DataFrame({"symbol": ["AAA", "BBB"], "exchange": ["HSX", "HNX"]})
    out = data_source._symbol_exchange_map(listing_fn=lambda: df)
    assert out == {"AAA": "HSX", "BBB": "HNX"}


def test_symbol_exchange_map_memoizes_same_day(monkeypatch):
    _reset_exchange_memo(monkeypatch)
    calls = {"n": 0}

    def listing_fn():
        calls["n"] += 1
        return pd.DataFrame({"symbol": ["AAA"], "exchange": ["HSX"]})

    data_source._symbol_exchange_map(listing_fn=listing_fn)
    data_source._symbol_exchange_map(listing_fn=listing_fn)
    assert calls["n"] == 1


def test_symbol_exchange_map_keeps_stale_on_error(monkeypatch):
    monkeypatch.setattr(data_source, "_exchange_map", {"AAA": "HSX"})
    monkeypatch.setattr(data_source, "_exchange_map_date", "2020-01-01")

    def boom():
        raise RuntimeError("net")

    out = data_source._symbol_exchange_map(listing_fn=boom)
    assert out == {"AAA": "HSX"}
```

- [ ] **Step 2: Chạy test để xác nhận FAIL**

Run: `python -m pytest test/test_data_source.py -k symbol_exchange_map -v`
Expected: FAIL với `AttributeError: module 'data_source' has no attribute '_symbol_exchange_map'`

- [ ] **Step 3: Cài đặt `_default_exchange_listing` + `_symbol_exchange_map`**

Trong `data_source.py`, chèn đoạn sau NGAY TRƯỚC định nghĩa `_default_price_board` hiện tại (dòng 31):

```python
_exchange_map: dict[str, str] = {}
_exchange_map_date = None


def _default_exchange_listing():
    from vnstock_data import Listing

    return Listing(source=VCI).symbols_by_exchange()


def _symbol_exchange_map(listing_fn: Callable = _default_exchange_listing) -> dict[str, str]:
    """Bản đồ symbol -> exchange, memoize theo NGÀY (cùng pattern _all_symbols
    ở market_refresher.py). Lỗi fetch -> giữ bản đồ cũ (nếu có), không raise,
    để _default_price_board tự xử lý mã lạ qua nhóm "unknown"."""
    global _exchange_map, _exchange_map_date
    today = dt.datetime.now(VN_TZ).strftime("%Y-%m-%d")
    if _exchange_map and _exchange_map_date == today:
        return _exchange_map
    try:
        df = listing_fn()
        mapping = dict(zip(df["symbol"], df["exchange"]))
    except BaseException as e:  # noqa: BLE001 — cố ý bắt cả SystemExit
        logger.warning("symbols_by_exchange thất bại: %s", _short(e))
        return _exchange_map
    if mapping:
        _exchange_map = mapping
        _exchange_map_date = today
    return _exchange_map
```

- [ ] **Step 4: Chạy test để xác nhận PASS**

Run: `python -m pytest test/test_data_source.py -k symbol_exchange_map -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add data_source.py test/test_data_source.py
git commit -m "feat: them ban do symbol->exchange memoize theo ngay"
```

---

### Task 2: Chia `_default_price_board` theo sàn, gộp kết quả từng phần

**Files:**
- Modify: `data_source.py:31-34` (thay thế toàn bộ định nghĩa `_default_price_board` hiện tại)
- Test: `test/test_data_source.py`

**Interfaces:**
- Consumes: `_symbol_exchange_map(listing_fn) -> dict[str, str]` (Task 1), helper test `_match_board_df(rows)` đã có sẵn trong `test/test_data_source.py` (dòng 166-178), `_reset_exchange_memo(monkeypatch)` (Task 1).
- Produces: `_default_board_fetch(symbols: list[str]) -> DataFrame`, `_group_symbols_by_exchange(symbols: list[str], exchange_map: dict[str, str]) -> dict[str, list[str]]`, `_default_price_board(symbols: list[str], board_fetch_fn: Callable = _default_board_fetch, listing_fn: Callable = _default_exchange_listing) -> DataFrame` (signature vị trí `(symbols)` giữ nguyên tương thích khi dùng làm default cho `price_board_fn` ở các hàm `fetch_*`).

- [ ] **Step 1: Viết test cho `_default_price_board` mới**

Thêm vào cuối `test/test_data_source.py` (cần `import pytest` ở đầu file — thêm dòng `import pytest` ngay dưới `import pandas as pd`):

```python
def test_default_price_board_splits_by_exchange(monkeypatch):
    """Chia mã theo sàn, gọi board_fetch_fn riêng từng nhóm thay vì 1 request
    gộp toàn bộ."""
    _reset_exchange_memo(monkeypatch)
    listing_df = pd.DataFrame(
        {"symbol": ["AAA", "BBB", "CCC"], "exchange": ["HSX", "HNX", "UPCOM"]}
    )
    calls = []

    def board_fetch_fn(symbols):
        calls.append(sorted(symbols))
        return _match_board_df([(s, 10.0, 11.0, 1.0) for s in symbols])

    data_source._default_price_board(
        ["AAA", "BBB", "CCC"], board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
    )
    assert sorted(calls) == [["AAA"], ["BBB"], ["CCC"]]


def test_default_price_board_merges_successful_groups(monkeypatch):
    _reset_exchange_memo(monkeypatch)
    listing_df = pd.DataFrame({"symbol": ["AAA", "BBB"], "exchange": ["HSX", "HNX"]})

    def board_fetch_fn(symbols):
        return _match_board_df([(s, 10.0, 11.0, 1.0) for s in symbols])

    out = data_source._default_price_board(
        ["AAA", "BBB"], board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
    )
    assert sorted(out[("listing", "symbol")].tolist()) == ["AAA", "BBB"]


def test_default_price_board_partial_failure_keeps_successful_groups(monkeypatch):
    """1 sàn lỗi (vd UPCOM timeout) không làm fail toàn bộ — vẫn gộp dữ liệu
    các sàn thành công."""
    _reset_exchange_memo(monkeypatch)
    listing_df = pd.DataFrame({"symbol": ["AAA", "BBB"], "exchange": ["HSX", "UPCOM"]})

    def board_fetch_fn(symbols):
        if symbols == ["BBB"]:
            raise RuntimeError("timeout")
        return _match_board_df([(s, 10.0, 11.0, 1.0) for s in symbols])

    out = data_source._default_price_board(
        ["AAA", "BBB"], board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
    )
    assert out[("listing", "symbol")].tolist() == ["AAA"]


def test_default_price_board_raises_when_all_groups_fail(monkeypatch):
    _reset_exchange_memo(monkeypatch)
    listing_df = pd.DataFrame({"symbol": ["AAA"], "exchange": ["HSX"]})

    def board_fetch_fn(symbols):
        raise RuntimeError("timeout")

    with pytest.raises(RuntimeError):
        data_source._default_price_board(
            ["AAA"], board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
        )


def test_default_price_board_unknown_exchange_still_included(monkeypatch):
    """Mã không tra được sàn (không có trong bản đồ) vẫn được gửi request
    riêng, không bị rớt khỏi kết quả."""
    _reset_exchange_memo(monkeypatch)
    listing_df = pd.DataFrame({"symbol": ["AAA"], "exchange": ["HSX"]})

    def board_fetch_fn(symbols):
        return _match_board_df([(s, 10.0, 11.0, 1.0) for s in symbols])

    out = data_source._default_price_board(
        ["AAA", "ZZZ"], board_fetch_fn=board_fetch_fn, listing_fn=lambda: listing_df
    )
    assert sorted(out[("listing", "symbol")].tolist()) == ["AAA", "ZZZ"]
```

- [ ] **Step 2: Chạy test để xác nhận FAIL**

Run: `python -m pytest test/test_data_source.py -k default_price_board -v`
Expected: FAIL — `board_fetch_fn`/`listing_fn` không phải tham số hợp lệ của `_default_price_board` hiện tại (TypeError: unexpected keyword argument)

- [ ] **Step 3: Thay thế `_default_price_board`**

Trong `data_source.py`, xóa định nghĩa hiện tại (dòng 31-34):

```python
def _default_price_board(symbols: list[str]):
    from vnstock_data import Trading

    return Trading(symbol=symbols[0], source=VCI).price_board(symbols)
```

Thay bằng:

```python
def _default_board_fetch(symbols: list[str]):
    from vnstock_data import Trading

    return Trading(symbol=symbols[0], source=VCI).price_board(symbols)


def _group_symbols_by_exchange(symbols: list[str], exchange_map: dict[str, str]) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for symbol in symbols:
        exch = exchange_map.get(symbol, "unknown")
        groups.setdefault(exch, []).append(symbol)
    return groups


def _default_price_board(
    symbols: list[str],
    board_fetch_fn: Callable = _default_board_fetch,
    listing_fn: Callable = _default_exchange_listing,
):
    """Chia symbols theo sàn (HSX/HNX/UPCOM; mã không tra được sàn -> nhóm
    "unknown") rồi gọi price_board riêng từng nhóm, gộp lại — 1 request cho
    ~1600 mã toàn TT dễ timeout/lỗi payload lớn. Nhóm lỗi bị bỏ qua (log
    warning); chỉ raise khi TẤT CẢ nhóm đều lỗi, giữ hợp đồng "fetch lỗi ->
    None" của các hàm fetch_* bao ngoài."""
    import pandas as pd

    exchange_map = _symbol_exchange_map(listing_fn)
    groups = _group_symbols_by_exchange(symbols, exchange_map)

    frames = []
    failed_exchanges = []
    for exch, group_symbols in groups.items():
        try:
            frames.append(board_fetch_fn(group_symbols))
        except BaseException as e:  # noqa: BLE001 — cố ý bắt cả SystemExit
            logger.warning("price_board sàn %s thất bại: %s", exch, _short(e))
            failed_exchanges.append(exch)

    if not frames:
        if failed_exchanges:
            raise RuntimeError(f"price_board thất bại toàn bộ sàn: {failed_exchanges}")
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
```

- [ ] **Step 4: Chạy test để xác nhận PASS**

Run: `python -m pytest test/test_data_source.py -k default_price_board -v`
Expected: 5 passed

- [ ] **Step 5: Chạy toàn bộ test suite của data_source để đảm bảo không phá vỡ hành vi cũ**

Run: `python -m pytest test/test_data_source.py -v`
Expected: tất cả pass, bao gồm các test `fetch_vn100_board`/`fetch_market_bid_ask`/`fetch_market_snapshot` hiện có (chúng inject `price_board_fn` trực tiếp nên không đụng code mới).

- [ ] **Step 6: Commit**

```bash
git add data_source.py test/test_data_source.py
git commit -m "feat: chia price_board theo san HSX/HNX/UPCOM, gop ket qua tung phan"
```

---

### Task 3: Cập nhật comment ngân sách call ở market_refresher.py

**Files:**
- Modify: `market_refresher.py:7-13`

**Interfaces:**
- Không có — chỉ sửa docstring/comment, không đổi code chạy.

- [ ] **Step 1: Sửa đoạn comment ngân sách**

Trong `market_refresher.py`, thay đoạn (dòng 7-13):

```
Ngân sách trong giờ GD: tick 1s × (1 price_board toàn TT + 1 history nến 1D
VNINDEX cho /quotes) = 120 call/phút — tick thường chỉ cập nhật snapshot quotes
(/quotes); mỗi tick thứ 20 (~20s) tái dùng CÙNG lần fetch đó dựng thêm views
market_wide + warm nến VNINDEX (3 call/phút, KHÔNG đổi so với bản 5s trước —
vẫn giữ nhịp 20s). Tổng ~123 call/phút — dưới hạn Golden 500 req/phút, nhưng
VƯỢT hạn Community 60 req/phút (chấp nhận được vì project chạy tier Golden,
xem vnstock_license; nếu rớt về Community phải tăng lại QUOTES_INTERVAL_S).
```

Bằng:

```
Ngân sách trong giờ GD: tick 1s × (3 price_board chia theo sàn HSX/HNX/UPCOM
+ 1 history nến 1D VNINDEX cho /quotes) = 240 call/phút — tick thường chỉ cập
nhật snapshot quotes (/quotes); mỗi tick thứ 20 (~20s) tái dùng CÙNG lần fetch
đó dựng thêm views market_wide + warm nến VNINDEX (3 call/phút, KHÔNG đổi so
với bản 5s trước — vẫn giữ nhịp 20s). Tổng ~243 call/phút — dưới hạn Golden
500 req/phút, nhưng VƯỢT hạn Community 60 req/phút (chấp nhận được vì project
chạy tier Golden, xem vnstock_license; nếu rớt về Community phải tăng lại
QUOTES_INTERVAL_S).
```

- [ ] **Step 2: Chạy toàn bộ test suite để đảm bảo không có gì gãy**

Run: `python -m pytest test/ -v`
Expected: tất cả pass (đổi comment, không đổi logic)

- [ ] **Step 3: Commit**

```bash
git add market_refresher.py
git commit -m "docs: cap nhat ngan sach call sau khi chia price_board theo san"
```

---

### Task 4: Xác minh cuối cùng

**Files:** không tạo/sửa file mới — chỉ chạy kiểm tra.

- [ ] **Step 1: Chạy toàn bộ test suite**

Run: `python -m pytest test/ -v`
Expected: tất cả pass, không có test nào bị skip ngoài ý muốn.

- [ ] **Step 2: Rà lại spec vs code**

Đối chiếu từng mục trong `docs/superpowers/specs/2026-07-19-price-board-split-by-exchange-design.md` với code đã viết (chia nhóm, gộp từng phần, raise khi toàn bộ lỗi, mã "unknown" không bị rớt, comment ngân sách đã cập nhật) — xác nhận không có mục nào bị bỏ sót.
