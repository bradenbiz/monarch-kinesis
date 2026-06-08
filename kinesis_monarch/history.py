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

# Extra fields used to reconstruct individual transactions (not just balances).
_TYPE_FIELDS = ("Transaction_Type", "transactionType", "type", "Type")
_START_FIELDS = ("Starting_Balance", "startingBalance", "Opening_Balance", "opening")
_TRADE_PRICE_FIELDS = ("Trade_Price", "tradePrice", "executionPrice")
_TOTAL_FIELDS = ("Total", "total")
_FEE_FIELDS = ("Fee", "fee")
_TXN_ID_FIELDS = ("Transaction_ID", "transactionId", "id", "txid")
_ORDER_ID_FIELDS = ("Order_ID", "orderId")
_PAIR_FIELDS = ("Currency_Pair", "currencyPair", "pair")


@dataclass(frozen=True)
class BalanceEvent:
    date: dt.date
    symbol: str
    running_balance: Optional[float]
    delta: Optional[float]


@dataclass(frozen=True)
class LedgerTxn:
    """One transaction-ledger entry, retaining the fields a transaction needs.

    `token_delta` is the signed change in the asset's units (closing - starting),
    which gives both magnitude and direction; `amount` is Kinesis's unsigned
    magnitude field, used only as a fallback when balances are missing.
    """

    date: dt.date
    symbol: str
    txn_type: str
    token_delta: Optional[float]
    amount: Optional[float]
    trade_price: Optional[float]
    total: Optional[float]
    fee: Optional[float]
    txn_id: str
    order_id: str
    pair: str
    raw_datetime: str = ""  # original DateTime string, for sub-day correlation


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


# ---- statement -> individual transactions ----------------------------------


def normalize_transactions(statement: Any) -> list[LedgerTxn]:
    """Tolerantly convert a balance-statement payload into LedgerTxns.

    Mirrors normalize_statement but keeps the richer per-row fields (type, trade
    price, USD total, fee, ids) needed to emit a transaction feed rather than a
    daily balance series.
    """
    txns: list[LedgerTxn] = []
    for entry, symbol_hint in _iter_entries(statement):
        if not isinstance(entry, dict):
            continue
        raw_dt = _first(entry, _DATE_FIELDS)
        date = parse_date(raw_dt)
        symbol = _first(entry, _SYMBOL_FIELDS) or symbol_hint
        if date is None or not symbol:
            continue
        closing = _to_float(_first(entry, _BALANCE_FIELDS))
        starting = _to_float(_first(entry, _START_FIELDS))
        token_delta = closing - starting if closing is not None and starting is not None else None
        txns.append(
            LedgerTxn(
                date=date,
                symbol=str(symbol).upper(),
                txn_type=str(_first(entry, _TYPE_FIELDS) or "").strip(),
                token_delta=token_delta,
                amount=_to_float(_first(entry, _DELTA_FIELDS + ("Amount",))),
                trade_price=_to_float(_first(entry, _TRADE_PRICE_FIELDS)),
                total=_to_float(_first(entry, _TOTAL_FIELDS)),
                fee=_to_float(_first(entry, _FEE_FIELDS)),
                txn_id=str(_first(entry, _TXN_ID_FIELDS) or ""),
                order_id=str(_first(entry, _ORDER_ID_FIELDS) or ""),
                pair=str(_first(entry, _PAIR_FIELDS) or ""),
                raw_datetime=str(raw_dt or ""),
            )
        )
    return txns


# ---- card-spend correlation ------------------------------------------------
#
# A Kinesis card payment is booked as three ledger rows at the SAME timestamp: a
# metal sell (e.g. KAU_C1USD, in the metal account), the matching C1USD buy, and
# a `Withdrawal(Card Payment)` debit (C1USD account) that carries the card's
# Transaction_ID. The merchant name lives only in the card app, not this API, so
# we link the metal sell to its card row to at least flag it and attach the id.


def is_card_payment(txn: "LedgerTxn") -> bool:
    return "card" in txn.txn_type.lower()


def card_spend_index(txns: Iterable["LedgerTxn"]) -> dict[str, "LedgerTxn"]:
    """Map each card-payment row's raw timestamp -> the card LedgerTxn."""
    return {t.raw_datetime: t for t in txns if is_card_payment(t) and t.raw_datetime}


def card_funder_for(
    txn: "LedgerTxn", card_index: dict[str, "LedgerTxn"], tol: float = 0.03
) -> Optional["LedgerTxn"]:
    """If `txn` is the metal sell that funded a card payment, return that card row.

    Match = a sell-side Trade on a USD-quoted pair whose timestamp equals a card
    payment's, and whose USD value is within `tol` of the card's charge (guards
    against a coincidental same-second trade). Returns None otherwise.
    """
    if txn.txn_type.lower() != "trade" or "USD" not in txn.pair.upper():
        return None
    if txn.token_delta is None or txn.token_delta >= 0:  # must be an outflow (sell)
        return None
    card = card_index.get(txn.raw_datetime)
    if card is None or card.amount is None:
        return None
    magnitude = abs(txn.token_delta)
    implied_usd = magnitude * txn.trade_price if txn.trade_price else None
    if implied_usd is None:
        return card  # no price to cross-check; timestamp match alone
    if abs(implied_usd - card.amount) <= max(0.5, tol * card.amount):
        return card
    return None


def humanize_txn_type(txn_type: str) -> str:
    """'Holder's_Distribution' -> 'Holder's Distribution'; blank -> 'Activity'."""
    pretty = (txn_type or "").replace("_", " ").strip()
    return pretty or "Activity"


def txn_usd_amount(txn: LedgerTxn, daily_price: Optional[float]) -> Optional[float]:
    """Signed USD value of one transaction, positive when the holding increased.

    USD magnitude is resolved in priority order: a trade's own execution price,
    then the row's USD `Total` (e.g. KVT deposits carry it), then the token
    magnitude x that day's price. Returns None when no USD value can be derived.
    """
    magnitude = (
        abs(txn.token_delta)
        if txn.token_delta is not None
        else (abs(txn.amount) if txn.amount is not None else None)
    )
    # Direction: balance up = money in (+), balance down = out (-).
    if txn.token_delta is not None and txn.token_delta != 0:
        sign = 1.0 if txn.token_delta > 0 else -1.0
    else:
        sign = 1.0

    usd: Optional[float] = None
    if txn.txn_type.lower() == "trade" and txn.trade_price and magnitude is not None:
        usd = magnitude * txn.trade_price
    elif txn.total is not None and txn.total != 0:
        usd = abs(txn.total)
    elif magnitude is not None and daily_price is not None:
        usd = magnitude * daily_price

    if usd is None:
        return None
    return round(sign * usd, 2)


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
