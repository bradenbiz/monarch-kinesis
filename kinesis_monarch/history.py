"""Reconstruct a daily USD value series per asset from Kinesis data.

The Kinesis statement/OHLC JSON shapes are only lightly documented, so every
parser here is deliberately tolerant of field-name and container variations.
The pure functions are exercised by tests/test_history.py against several
plausible response shapes. If the live API differs, run `inspect` (see cli.py)
and extend the candidate field lists below.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Iterable, Optional

# Candidate field names, in priority order, for tolerant extraction.
# (Includes the exact field names the live Kinesis API returns: DateTime,
# Currency_Code, Closing_Balance.)
_DATE_FIELDS = ("DateTime", "date", "transactionDate", "createdAt", "created_at", "timestamp", "time")
_SYMBOL_FIELDS = ("Currency_Code", "symbol", "currency", "asset", "coin", "code")
_BALANCE_FIELDS = ("Closing_Balance", "balance", "runningBalance", "closingBalance", "balanceAfter", "closing")
# NOTE: Kinesis's 'Amount' is an UNSIGNED magnitude, so we deliberately do NOT
# treat it as a signed delta. For Kinesis, 'Closing_Balance' (a running balance)
# is used instead and takes precedence below. These lowercase fields only apply
# to other/generic statement shapes; they won't match Kinesis's capitalized keys.
_DELTA_FIELDS = ("amount", "change", "quantity", "qty", "value", "delta")
_CLOSE_FIELDS = ("close", "c", "closePrice", "price")
_LIST_KEYS = ("data", "statements", "statement", "entries", "rows", "items", "results", "transactions")


@dataclass(frozen=True)
class BalanceEvent:
    date: dt.date
    symbol: str
    running_balance: Optional[float]
    delta: Optional[float]


# ---- primitive coercion ----------------------------------------------------


def parse_date(value: Any) -> Optional[dt.date]:
    """Parse ISO strings or epoch (s / ms) into a date. None on failure."""
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:  # milliseconds
            ts /= 1000.0
        try:
            return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).date()
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.isdigit():
            return parse_date(int(s))
        s = s.replace("Z", "+00:00")
        try:
            return dt.datetime.fromisoformat(s).date()
        except ValueError:
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y"):
                try:
                    return dt.datetime.strptime(s[:10], fmt).date()
                except ValueError:
                    continue
    return None


def _to_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip().replace(",", "")
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _first(entry: dict, fields: Iterable[str]) -> Any:
    for f in fields:
        if f in entry and entry[f] not in (None, ""):
            return entry[f]
    return None


# ---- statement -> balance events -------------------------------------------


def _iter_entries(statement: Any, inherited_symbol: Optional[str] = None):
    """Yield (entry_dict, symbol_hint) pairs from many container shapes."""
    if statement is None:
        return
    if isinstance(statement, list):
        for item in statement:
            yield from _iter_entries(item, inherited_symbol)
        return
    if isinstance(statement, dict):
        # A nested container keyed by one of the known list keys.
        for key in _LIST_KEYS:
            if key in statement and isinstance(statement[key], (list, dict)):
                yield from _iter_entries(statement[key], inherited_symbol)
                return
        # A dict mapping symbol -> entries (e.g. {"KAU": [...], "KAG": [...]}).
        looks_like_symbol_map = all(
            isinstance(v, (list, dict)) for v in statement.values()
        ) and statement and not (_first(statement, _DATE_FIELDS))
        if looks_like_symbol_map:
            for sym, sub in statement.items():
                yield from _iter_entries(sub, sym)
            return
        # Otherwise treat it as a single ledger entry.
        yield statement, inherited_symbol


def normalize_statement(statement: Any) -> list[BalanceEvent]:
    """Tolerantly convert a balance-statement payload into BalanceEvents."""
    events: list[BalanceEvent] = []
    for entry, symbol_hint in _iter_entries(statement):
        if not isinstance(entry, dict):
            continue
        date = parse_date(_first(entry, _DATE_FIELDS))
        symbol = _first(entry, _SYMBOL_FIELDS) or symbol_hint
        if date is None or not symbol:
            continue
        running = _to_float(_first(entry, _BALANCE_FIELDS))
        delta = _to_float(_first(entry, _DELTA_FIELDS))
        # credit/debit pair fallback for the delta.
        if delta is None:
            credit = _to_float(entry.get("credit"))
            debit = _to_float(entry.get("debit"))
            if credit is not None or debit is not None:
                delta = (credit or 0.0) - (debit or 0.0)
        events.append(
            BalanceEvent(date=date, symbol=str(symbol).upper(), running_balance=running, delta=delta)
        )
    return events


def symbols_in(events: Iterable[BalanceEvent]) -> list[str]:
    return sorted({e.symbol for e in events})


def daily_balances(
    events: Iterable[BalanceEvent], symbol: str, end: dt.date
) -> dict[dt.date, float]:
    """Step-function daily balance for one symbol, from its first event to `end`.

    Uses explicit running-balance fields when present, otherwise accumulates
    deltas. Multiple events on a day collapse to that day's final balance.
    """
    rows = sorted((e for e in events if e.symbol == symbol), key=lambda e: e.date)
    if not rows:
        return {}

    # Resolve each event date to an end-of-day balance.
    by_day: dict[dt.date, float] = {}
    running = 0.0
    have_running = False
    for e in rows:
        if e.running_balance is not None:
            running = e.running_balance
            have_running = True
        elif e.delta is not None:
            running += e.delta
            have_running = True
        # if neither present, carry previous running forward
        by_day[e.date] = running

    if not have_running:
        return {}

    start = rows[0].date
    if end < start:
        end = start
    series: dict[dt.date, float] = {}
    cur = start
    last = by_day.get(start, 0.0)
    one_day = dt.timedelta(days=1)
    while cur <= end:
        if cur in by_day:
            last = by_day[cur]
        series[cur] = last
        cur += one_day
    return series


# ---- OHLC -> daily close prices --------------------------------------------


def extract_daily_closes(ohlc: Any) -> dict[dt.date, float]:
    """Bucket OHLC candles into one close price per calendar day (last wins).

    Handles both array candles ([time, open, high, low, close, ...]) and object
    candles ({time/date, open, high, low, close}).
    """
    closes: dict[dt.date, float] = {}
    candles = ohlc
    if isinstance(ohlc, dict):
        for key in _LIST_KEYS + ("ohlc", "candles", "prices"):
            if isinstance(ohlc.get(key), list):
                candles = ohlc[key]
                break
    if not isinstance(candles, list):
        return closes

    for c in candles:
        date: Optional[dt.date] = None
        close: Optional[float] = None
        if isinstance(c, dict):
            date = parse_date(_first(c, _DATE_FIELDS))
            close = _to_float(_first(c, _CLOSE_FIELDS))
        elif isinstance(c, (list, tuple)) and len(c) >= 5:
            date = parse_date(c[0])
            close = _to_float(c[4])  # [time, open, high, low, close, (volume)]
        if date is not None and close is not None:
            closes[date] = close  # later candle for same day overwrites -> last close
    return closes


def forward_filled_prices(
    closes: dict[dt.date, float], start: dt.date, end: dt.date
) -> dict[dt.date, float]:
    """Daily price for every day in [start, end], carrying the last known close.

    Leading days with no prior price are back-filled with the first known close.
    """
    if not closes:
        return {}
    known = sorted(closes.items())
    first_price = known[0][1]
    out: dict[dt.date, float] = {}
    cur = start
    last = first_price
    one_day = dt.timedelta(days=1)
    while cur <= end:
        if cur in closes:
            last = closes[cur]
        out[cur] = last
        cur += one_day
    return out


# ---- combine ---------------------------------------------------------------


def build_value_series(
    daily_balance: dict[dt.date, float],
    daily_price: dict[dt.date, float],
) -> list[tuple[dt.date, float]]:
    """value(day) = balance(day) * price(day), over the balance's date range.

    For fiat (price == 1 everywhere) this is just the balance. Days missing a
    price are skipped (rare; only at the very start before any candle exists).
    """
    out: list[tuple[dt.date, float]] = []
    for day in sorted(daily_balance):
        price = daily_price.get(day)
        if price is None:
            continue
        out.append((day, round(daily_balance[day] * price, 2)))
    return out
