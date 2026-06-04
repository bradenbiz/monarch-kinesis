"""High-level pipeline: read Kinesis, build value history, write to Monarch."""

from __future__ import annotations

import datetime as dt
import io
from dataclasses import dataclass, field
from typing import Any, Optional

from . import assets, history
from .config import Config
from .kinesis import KinesisClient, KinesisError

# Metals are the default focus (the user models "separate per metal").
DEFAULT_SYMBOLS = ["KAU", "KAG"]

# The statement endpoint returns only ~30 days unless given a date range, so we
# always request from well before Kinesis launched to capture full history.
FULL_HISTORY_START = dt.date(2017, 1, 1)

_PRICE_FIELDS = ("mid_price", "midPrice", "mid", "price", "value", "rate", "last")


def today_utc() -> dt.date:
    return dt.datetime.now(dt.timezone.utc).date()


def _iso(day: dt.date, end_of_day: bool = False) -> str:
    t = "23:59:59.999" if end_of_day else "00:00:00.000"
    return f"{day.isoformat()}T{t}Z"


def extract_price(payload: Any) -> Optional[float]:
    """Tolerantly pull a single price number from a mid-price response."""
    if isinstance(payload, (int, float)):
        return float(payload)
    if isinstance(payload, str):
        return history._to_float(payload)
    if isinstance(payload, dict):
        for f in _PRICE_FIELDS:
            if f in payload:
                v = history._to_float(payload[f])
                if v is not None:
                    return v
        # bid/ask midpoint fallback
        bid = history._to_float(payload.get("bid"))
        ask = history._to_float(payload.get("ask"))
        if bid is not None and ask is not None:
            return (bid + ask) / 2.0
    return None


@dataclass
class SymbolResult:
    symbol: str
    account_name: str
    current_amount: float
    current_value: Optional[float]
    history_rows: list[tuple[dt.date, float]] = field(default_factory=list)
    note: str = ""

    @property
    def csv(self) -> str:
        return build_csv(self.account_name, self.history_rows)


def build_csv(account_name: str, rows: list[tuple[dt.date, float]]) -> str:
    """Monarch balance-history CSV: header `Date,Balance,Account`."""
    buf = io.StringIO()
    buf.write("Date,Balance,Account\n")
    for day, value in rows:
        buf.write(f"{day.isoformat()},{value:.2f},{account_name}\n")
    return buf.getvalue()


def _daily_price_for_symbol(
    client: KinesisClient,
    config: Config,
    symbol: str,
    start: dt.date,
    end: dt.date,
) -> dict[dt.date, float]:
    """Daily price (in USD) for every day in [start, end]."""
    if assets.is_usd_pegged(symbol):
        cur = start
        out: dict[dt.date, float] = {}
        one = dt.timedelta(days=1)
        while cur <= end:
            out[cur] = 1.0
            cur += one
        return out

    # Kinesis migrated its USD quote from plain USD to the C1USD stablecoin at
    # some point. They are 1:1 dollar-equivalent, so old price history lives
    # under {SYM}_USD and recent under {SYM}_C1USD. Fetch BOTH and merge for a
    # continuous series. Order matters: the configured quote (default C1USD) is
    # fetched LAST so it wins on any overlapping days.
    quote_order: list[str] = []
    for q in ("USD", "C1USD", config.kinesis_quote):
        if q and q not in quote_order:
            quote_order.append(q)
    # Move the primary configured quote to the end (overlap winner).
    if config.kinesis_quote in quote_order:
        quote_order.remove(config.kinesis_quote)
        quote_order.append(config.kinesis_quote)

    closes: dict[dt.date, float] = {}
    for quote in quote_order:
        pair = assets.pair_for(symbol, quote)
        closes.update(_fetch_closes(client, config, pair, start, end))
    return history.forward_filled_prices(closes, start, end)


def _fetch_closes(
    client: KinesisClient, config: Config, pair: str, start: dt.date, end: dt.date
) -> dict[dt.date, float]:
    """Chunked, resilient OHLC daily-close fetch for one pair over [start, end].

    Pulls ~180-day windows (exchanges cap candles per request; a multi-year
    single pull can truncate or 500). A failed/empty window is simply skipped.
    """
    closes: dict[dt.date, float] = {}
    window = dt.timedelta(days=180)
    cur = start
    while cur <= end:
        chunk_end = min(cur + window, end)
        try:
            ohlc = client.get_ohlc(
                pair, _iso(cur), _iso(chunk_end, end_of_day=True), config.ohlc_timeframe
            )
            closes.update(history.extract_daily_closes(ohlc))
        except KinesisError:
            pass  # skip this window; merge/forward-fill bridges it
        cur = chunk_end + dt.timedelta(days=1)
    return closes


def build_symbol_result(
    client: KinesisClient,
    config: Config,
    symbol: str,
    holdings: dict[str, float],
    events: list[history.BalanceEvent],
    overall_start: Optional[dt.date] = None,
    assume_constant: bool = False,
) -> SymbolResult:
    symbol = symbol.upper()
    name = assets.account_name(symbol)
    end = today_utc()
    current_amount = holdings.get(symbol, 0.0)

    # Reconstruct the dated holdings series.
    daily_bal = history.daily_balances(events, symbol, end)
    # The live /holdings figure is the authoritative current balance, so pin
    # today's point to it (statement Closing_Balance can lag post-trade minting).
    if daily_bal:
        daily_bal[end] = current_amount

    rows: list[tuple[dt.date, float]] = []
    note = ""
    if daily_bal:
        start = min(daily_bal)
        try:
            daily_price = _daily_price_for_symbol(client, config, symbol, start, end)
        except KinesisError as exc:
            daily_price = {}
            note = f"price lookup failed ({exc}); history skipped"
        if daily_price:
            rows = history.build_value_series(daily_bal, daily_price)
        elif not note:
            note = "no price data returned; history skipped"
    elif assume_constant and current_amount and not assets.is_usd_pegged(symbol) and overall_start:
        # Opt-in fallback ONLY (not used by default): if a symbol has no real
        # transaction history anywhere, approximate value over time as the
        # CURRENT holding held constant across the window x daily price.
        try:
            daily_price = _daily_price_for_symbol(client, config, symbol, overall_start, end)
        except KinesisError as exc:
            daily_price = {}
            note = f"price lookup failed ({exc}); current balance only"
        if daily_price:
            const_bal = {day: current_amount for day in daily_price}
            rows = history.build_value_series(const_bal, daily_price)
            note = "no transaction history; charted as constant current holdings x daily price"
        elif not note:
            note = "no history and no price data; current balance only"
    else:
        note = "no statement history found; only current balance will be set"

    # Current value: prefer the last history row, else value the live holding.
    current_value: Optional[float] = None
    if rows:
        current_value = rows[-1][1]
    elif current_amount:
        if assets.is_usd_pegged(symbol):
            current_value = round(current_amount, 2)
        else:
            try:
                price = extract_price(
                    client.get_mid_price(assets.pair_for(symbol, config.kinesis_quote))
                )
                if price is not None:
                    current_value = round(current_amount * price, 2)
            except KinesisError as exc:
                note = (note + "; " if note else "") + f"current price lookup failed ({exc})"

    return SymbolResult(
        symbol=symbol,
        account_name=name,
        current_amount=current_amount,
        current_value=current_value,
        history_rows=rows,
        note=note,
    )


def resolve_symbols(
    requested: Optional[list[str]],
    holdings: dict[str, float],
    events: list[history.BalanceEvent],
    include_all: bool,
) -> list[str]:
    """Decide which symbols to sync."""
    if requested:
        return [s.upper() for s in requested]
    if include_all:
        found = set(holdings) | set(history.symbols_in(events))
        # Keep symbols with any current holding or any history.
        return sorted(s for s in found if holdings.get(s, 0.0) or s in history.symbols_in(events))
    # Default: the metals, but only those the account actually has data for.
    have = set(holdings) | set(history.symbols_in(events))
    return [s for s in DEFAULT_SYMBOLS if s in have] or DEFAULT_SYMBOLS


def gather(
    config: Config,
    requested_symbols: Optional[list[str]] = None,
    include_all: bool = False,
) -> list[SymbolResult]:
    """Read Kinesis and build per-symbol value histories (no Monarch writes)."""
    client = KinesisClient(
        config.kinesis_public_key, config.kinesis_private_key, config.kinesis_base_url
    )
    holdings = assets.parse_holdings(client.get_holdings())
    # Fetch FULL history via the fromDate/toDate params (without them the
    # endpoint caps at ~30 days, dropping older trades incl. KVT).
    statement = client.get_balance_statement(
        params={
            "fromDate": _iso(FULL_HISTORY_START),
            "toDate": _iso(today_utc(), end_of_day=True),
        }
    )
    events = history.normalize_statement(statement)

    # CLI --symbols wins; otherwise fall back to the SYMBOLS env default.
    if requested_symbols is None and config.symbols:
        requested_symbols = config.symbols

    # Earliest dated activity across the whole account — used to align the
    # constant-holdings chart for symbols that have no transaction history.
    overall_start = min((e.date for e in events), default=None)

    symbols = resolve_symbols(requested_symbols, holdings, events, include_all)
    return [
        build_symbol_result(client, config, sym, holdings, events, overall_start)
        for sym in symbols
    ]
