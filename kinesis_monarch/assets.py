"""Asset naming and current-holdings parsing."""

from __future__ import annotations

from typing import Any

from .history import _to_float, _LIST_KEYS  # reuse tolerant helpers

# Common fiat currencies on Kinesis; valued 1:1 in their own units.
FIAT = {"USD", "EUR", "GBP", "AUD", "CAD", "CHF", "JPY", "NZD", "SGD", "HKD", "AED"}

# Symbols that are effectively 1 USD each (plain USD + Kinesis's C1USD unit).
# These are valued 1:1 in dollars without needing a price lookup.
USD_PEGGED = {"USD", "C1USD"}

_PRETTY = {
    "KAU": "Kinesis Gold (KAU)",
    "KAG": "Kinesis Silver (KAG)",
    "KVT": "Kinesis Velocity Token (KVT)",
}

_AMOUNT_FIELDS = ("amount", "balance", "available", "total", "quantity", "value", "free")
_SYMBOL_FIELDS = ("symbol", "currency", "asset", "coin", "code")


def account_name(symbol: str) -> str:
    """Display name for the Monarch manual account representing this symbol."""
    symbol = symbol.upper()
    return _PRETTY.get(symbol, f"Kinesis {symbol}")


def is_fiat(symbol: str) -> bool:
    return symbol.upper() in FIAT


def is_usd_pegged(symbol: str) -> bool:
    """True for symbols worth ~1 USD each (USD, C1USD) — valued 1:1, no price lookup."""
    return symbol.upper() in USD_PEGGED


def pair_for(symbol: str, base_fiat: str) -> str:
    return f"{symbol.upper()}_{base_fiat.upper()}"


def parse_holdings(holdings: Any) -> dict[str, float]:
    """Tolerantly extract {SYMBOL: amount} from a holdings payload.

    Handles: list of {symbol, amount}; dict mapping symbol -> amount; dict
    mapping symbol -> {amount/balance/...}; or any of those wrapped in a
    'data'/'holdings'/... container key.
    """
    # Unwrap a known container key.
    if isinstance(holdings, dict):
        for key in _LIST_KEYS + ("holdings", "balances", "wallets"):
            if key in holdings and isinstance(holdings[key], (list, dict)):
                holdings = holdings[key]
                break

    out: dict[str, float] = {}

    if isinstance(holdings, list):
        for item in holdings:
            if not isinstance(item, dict):
                continue
            symbol = _first_str(item, _SYMBOL_FIELDS)
            amount = _to_float(_first_val(item, _AMOUNT_FIELDS))
            if symbol and amount is not None:
                out[symbol.upper()] = amount
    elif isinstance(holdings, dict):
        for sym, val in holdings.items():
            if isinstance(val, dict):
                amount = _holding_total(val)
            else:
                amount = _to_float(val)
            if amount is not None:
                out[str(sym).upper()] = amount

    return out


def _holding_total(val: dict) -> float | None:
    """Total units for one holding.

    Kinesis returns {"available": x, "allocatedOnExchange": y}; the true holding
    is available + allocated (allocated = units tied up in open orders). Falls
    back to the first generic amount field for other shapes.
    """
    available = _to_float(val.get("available"))
    allocated = _to_float(val.get("allocatedOnExchange"))
    if available is not None or allocated is not None:
        return (available or 0.0) + (allocated or 0.0)
    return _to_float(_first_val(val, _AMOUNT_FIELDS))


def _first_val(entry: dict, fields):
    for f in fields:
        if f in entry and entry[f] not in (None, ""):
            return entry[f]
    return None


def _first_str(entry: dict, fields):
    v = _first_val(entry, fields)
    return str(v) if v is not None else None
