import json
import os
from datetime import datetime, timedelta, timezone

import pandas as pd

from app.data import market_cache

VN_TZ = timezone(timedelta(hours=7))
RUNTIME_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
)
FOREIGN_TRADING_HISTORY_CACHE_FILE = os.environ.get(
    "FOREIGN_TRADING_HISTORY_CACHE_FILE",
    os.path.join(RUNTIME_DATA_DIR, "foreign_trading_history_cache.json"),
)
MAX_HISTORY_LENGTH = 30


def _history_item(date, buy_value, sell_value, net_value, total_value):
    return {
        "date": date,
        "buy_value": int(buy_value),
        "sell_value": int(sell_value),
        "net_value": int(net_value),
        "total_value": int(total_value),
    }


def _save_history_cache(history):
    try:
        with open(FOREIGN_TRADING_HISTORY_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"history": history}, f, ensure_ascii=False)
    except OSError:
        pass


def load_history_cache():
    try:
        with open(FOREIGN_TRADING_HISTORY_CACHE_FILE, encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, ValueError):
        return []

    if not isinstance(payload, dict):
        return []

    history = payload.get("history")
    if not isinstance(history, list):
        return []

    validated = []
    for item in history:
        if not isinstance(item, dict):
            continue
        date = item.get("date")
        if not isinstance(date, str) or not date:
            continue
        buy_value = int(item.get("buy_value") or 0)
        sell_value = int(item.get("sell_value") or 0)
        net_value = int(item.get("net_value") or 0)
        total_value = int(item.get("total_value") or 0)
        validated.append(_history_item(date, buy_value, sell_value, net_value, total_value))

    market_cache.set_snapshot("foreign_trading_history", validated)
    return validated


def get_history():
    history = market_cache.get_snapshot("foreign_trading_history")
    if history is None:
        return load_history_cache()
    return history


def fetch_history_from_vendor(limit_days: int = MAX_HISTORY_LENGTH):
    """Lấy lịch sử khối ngoại từ vnstock_data.foreign_trade (nhiều ngày)."""
    try:
        from vnstock_data import Trading
    except Exception:
        return []

    try:
        df = Trading(source="VCI").foreign_trade()
    except Exception:
        return []

    if df is None or getattr(df, "empty", False):
        return []

    if not isinstance(df, pd.DataFrame):
        return []

    if "trading_date" not in df.columns:
        return []

    normalized = []
    for _, row in df.head(limit_days).iterrows():
        date = row.get("trading_date")
        if pd.isna(date):
            continue
        date_str = str(date)[:10]
        buy_value = row.get("fr_buy_value_total") or 0
        sell_value = row.get("fr_sell_value_total") or 0
        net_value = row.get("fr_net_value_total") or (buy_value - sell_value)
        total_value = buy_value + sell_value
        normalized.append(
            _history_item(
                date_str,
                buy_value,
                sell_value,
                net_value,
                total_value,
            )
        )

    return list(reversed(normalized))


def aggregate_foreign_totals(board_rows):
    buy_value = 0
    sell_value = 0
    if not isinstance(board_rows, list):
        return _history_item(datetime.now(VN_TZ).date().isoformat(), 0, 0, 0, 0)

    for row in board_rows:
        if not isinstance(row, dict):
            continue
        buy_value += int(row.get("foreign_buy_value") or 0)
        sell_value += int(row.get("foreign_sell_value") or 0)

    net_value = buy_value - sell_value
    total_value = buy_value + sell_value
    return _history_item(
        datetime.now(VN_TZ).date().isoformat(),
        buy_value,
        sell_value,
        net_value,
        total_value,
    )


def build_current_summary(board_rows):
    return aggregate_foreign_totals(board_rows)


def get_current_summary():
    board_rows = market_cache.get_snapshot("market_board_full")
    if isinstance(board_rows, list):
        return aggregate_foreign_totals(board_rows)

    history = get_history()
    if history:
        return history[-1]

    return _history_item(datetime.now(VN_TZ).date().isoformat(), 0, 0, 0, 0)


def get_history_with_fallback():
    history = get_history()
    if history and len(history) >= 2:
        return history

    vendor_history = fetch_history_from_vendor()
    if vendor_history:
        market_cache.set_snapshot("foreign_trading_history", vendor_history)
        _save_history_cache(vendor_history)
        return vendor_history

    return history


def update_history_from_board(board_rows):
    summary = aggregate_foreign_totals(board_rows)
    if summary["buy_value"] == 0 and summary["sell_value"] == 0:
        return False

    history = get_history() or []
    existing_index = next(
        (index for index, item in enumerate(history) if item.get("date") == summary["date"]),
        None,
    )

    if existing_index is not None:
        history[existing_index] = summary
    else:
        history.append(summary)
        history = history[-MAX_HISTORY_LENGTH:]

    market_cache.set_snapshot("foreign_trading_history", history)
    _save_history_cache(history)
    return True
