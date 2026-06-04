"""Tests for the pure history-reconstruction logic.

These exercise the tolerant parsers against several plausible Kinesis response
shapes, since the live JSON shapes are only lightly documented.
"""

import datetime as dt

from kinesis_monarch import assets, connector, history

D = dt.date


# ---- parse_date ------------------------------------------------------------


def test_parse_date_iso_and_epoch():
    assert history.parse_date("2022-04-01T00:00:00.000Z") == D(2022, 4, 1)
    assert history.parse_date("2022-04-01") == D(2022, 4, 1)
    assert history.parse_date(1648771200000) == D(2022, 4, 1)  # ms
    assert history.parse_date(1648771200) == D(2022, 4, 1)  # seconds
    assert history.parse_date("") is None
    assert history.parse_date(None) is None


# ---- normalize_statement ---------------------------------------------------


def test_statement_list_with_running_balance():
    statement = [
        {"date": "2022-01-01", "symbol": "KAU", "balance": "10"},
        {"date": "2022-01-05", "symbol": "KAU", "balance": "12.5"},
        {"date": "2022-01-03", "symbol": "KAG", "balance": "100"},
    ]
    events = history.normalize_statement(statement)
    assert len(events) == 3
    assert set(history.symbols_in(events)) == {"KAU", "KAG"}
    kau = [e for e in events if e.symbol == "KAU"]
    assert all(e.running_balance is not None for e in kau)


def test_statement_symbol_map_with_deltas():
    statement = {
        "KAU": [
            {"date": "2022-01-01", "amount": 5},
            {"date": "2022-01-10", "amount": 3},
        ],
        "KAG": [{"date": "2022-02-01", "amount": 50}],
    }
    events = history.normalize_statement(statement)
    assert set(history.symbols_in(events)) == {"KAU", "KAG"}
    bal = history.daily_balances(events, "KAU", D(2022, 1, 15))
    assert bal[D(2022, 1, 1)] == 5
    assert bal[D(2022, 1, 9)] == 5
    assert bal[D(2022, 1, 10)] == 8  # 5 + 3
    assert bal[D(2022, 1, 15)] == 8  # carried forward


def test_statement_wrapped_in_data_key_with_credit_debit():
    statement = {
        "data": [
            {"transactionDate": "2022-03-01", "currency": "KAU", "credit": 2, "debit": 0},
            {"transactionDate": "2022-03-02", "currency": "KAU", "credit": 0, "debit": 1},
        ]
    }
    events = history.normalize_statement(statement)
    bal = history.daily_balances(events, "KAU", D(2022, 3, 5))
    assert bal[D(2022, 3, 1)] == 2
    assert bal[D(2022, 3, 2)] == 1  # 2 - 1
    assert bal[D(2022, 3, 5)] == 1


def test_daily_balances_running_overrides_delta():
    events = [
        history.BalanceEvent(D(2022, 1, 1), "KAU", running_balance=10.0, delta=None),
        history.BalanceEvent(D(2022, 1, 2), "KAU", running_balance=None, delta=5.0),
    ]
    bal = history.daily_balances(events, "KAU", D(2022, 1, 2))
    assert bal[D(2022, 1, 1)] == 10.0
    assert bal[D(2022, 1, 2)] == 15.0


# ---- OHLC ------------------------------------------------------------------


def test_extract_daily_closes_object_candles_last_wins():
    ohlc = [
        {"date": "2022-01-01T00:00:00Z", "open": 1, "high": 2, "low": 0.5, "close": 1.5},
        {"date": "2022-01-01T12:00:00Z", "open": 1.5, "high": 2, "low": 1, "close": 1.8},
        {"date": "2022-01-02T00:00:00Z", "open": 1.8, "high": 2, "low": 1, "close": 1.9},
    ]
    closes = history.extract_daily_closes(ohlc)
    assert closes[D(2022, 1, 1)] == 1.8  # last candle of the day
    assert closes[D(2022, 1, 2)] == 1.9


def test_extract_daily_closes_array_candles():
    ohlc = [
        [1640995200000, 1, 2, 0.5, 1.5, 100],  # 2022-01-01
        [1641081600000, 1.5, 2, 1, 1.7, 100],  # 2022-01-02
    ]
    closes = history.extract_daily_closes(ohlc)
    assert closes[D(2022, 1, 1)] == 1.5
    assert closes[D(2022, 1, 2)] == 1.7


def test_extract_daily_closes_wrapped():
    ohlc = {"data": [{"date": "2022-01-01", "close": 2.0}]}
    closes = history.extract_daily_closes(ohlc)
    assert closes[D(2022, 1, 1)] == 2.0


def test_forward_filled_prices_carries_and_backfills():
    closes = {D(2022, 1, 2): 10.0, D(2022, 1, 4): 12.0}
    prices = history.forward_filled_prices(closes, D(2022, 1, 1), D(2022, 1, 5))
    assert prices[D(2022, 1, 1)] == 10.0  # backfilled with first known
    assert prices[D(2022, 1, 2)] == 10.0
    assert prices[D(2022, 1, 3)] == 10.0  # carried
    assert prices[D(2022, 1, 4)] == 12.0
    assert prices[D(2022, 1, 5)] == 12.0


# ---- combine ---------------------------------------------------------------


def test_build_value_series():
    daily_bal = {D(2022, 1, 1): 10.0, D(2022, 1, 2): 10.0}
    daily_price = {D(2022, 1, 1): 2.0, D(2022, 1, 2): 3.0}
    series = connector.build_csv  # ensure importable
    rows = history.build_value_series(daily_bal, daily_price)
    assert rows == [(D(2022, 1, 1), 20.0), (D(2022, 1, 2), 30.0)]


def test_build_csv_format():
    rows = [(D(2022, 1, 1), 20.0), (D(2022, 1, 2), 30.5)]
    csv = connector.build_csv("Kinesis Gold (KAU)", rows)
    lines = csv.strip().splitlines()
    assert lines[0] == "Date,Balance,Account"
    assert lines[1] == "2022-01-01,20.00,Kinesis Gold (KAU)"
    assert lines[2] == "2022-01-02,30.50,Kinesis Gold (KAU)"


# ---- holdings + naming -----------------------------------------------------


def test_parse_holdings_list():
    holdings = [
        {"symbol": "KAU", "amount": "1.2345"},
        {"currency": "KAG", "available": 50},
    ]
    parsed = assets.parse_holdings(holdings)
    assert parsed["KAU"] == 1.2345
    assert parsed["KAG"] == 50.0


def test_parse_holdings_symbol_map():
    holdings = {"KAU": {"balance": 2.0}, "USD": 100.0}
    parsed = assets.parse_holdings(holdings)
    assert parsed["KAU"] == 2.0
    assert parsed["USD"] == 100.0


def test_parse_holdings_wrapped():
    holdings = {"holdings": [{"symbol": "KAU", "total": 3}]}
    assert assets.parse_holdings(holdings)["KAU"] == 3.0


def test_account_naming():
    assert assets.account_name("KAU") == "Kinesis Gold (KAU)"
    assert assets.account_name("KAG") == "Kinesis Silver (KAG)"
    assert assets.account_name("BTC") == "Kinesis BTC"


def test_extract_price_variants():
    assert connector.extract_price(2.5) == 2.5
    assert connector.extract_price({"price": "2.5"}) == 2.5
    assert connector.extract_price({"midPrice": 3}) == 3.0
    # Real Kinesis mid-price shape uses 'mid_price' (underscore) + bid/ask.
    assert connector.extract_price({"symbol": "KAU_C1USD", "bid": 143.88, "ask": 144.09, "mid_price": 143.9878057}) == 143.9878057
    assert connector.extract_price({"bid": 2, "ask": 4}) == 3.0
    assert connector.extract_price({"nope": 1}) is None


def test_resolve_symbols_defaults_to_metals_with_data():
    holdings = {"KAU": 1.0, "USD": 100.0}
    events = history.normalize_statement([{"date": "2022-01-01", "symbol": "KAU", "balance": 1.0}])
    syms = connector.resolve_symbols(None, holdings, events, include_all=False)
    assert syms == ["KAU"]  # KAG has no data; USD excluded by metals default


def test_resolve_symbols_all_includes_held():
    holdings = {"KAU": 1.0, "USD": 100.0}
    syms = connector.resolve_symbols(None, holdings, [], include_all=True)
    assert set(syms) == {"KAU", "USD"}


# ---- REAL Kinesis API shapes (captured from live responses) -----------------


def test_real_kinesis_statement_uses_closing_balance():
    """Live statement: DateTime / Currency_Code / Closing_Balance, Amount unsigned."""
    statement = [
        {
            "DateTime": "2026-05-05 16:22:31 UTC",
            "Currency_Code": "KAU",
            "Transaction_Type": "Trade",
            "Amount": 0.0793810314,  # unsigned magnitude — must be ignored
            "Starting_Balance": "109.04312",
            "Closing_Balance": "108.96373",
        },
        {
            "DateTime": "2026-05-06 17:44:52 UTC",
            "Currency_Code": "KAU",
            "Transaction_Type": "Trade",
            "Amount": 0.0774335811,
            "Starting_Balance": "108.96373",
            "Closing_Balance": "108.88629",
        },
        {  # a different currency in the same ledger
            "DateTime": "2026-05-06 17:44:52 UTC",
            "Currency_Code": "C1USD",
            "Amount": 11.62,
            "Closing_Balance": "151.3636610",
        },
    ]
    events = history.normalize_statement(statement)
    assert set(history.symbols_in(events)) == {"KAU", "C1USD"}
    # Closing_Balance is used as the running balance (NOT the unsigned Amount).
    bal = history.daily_balances(events, "KAU", D(2026, 5, 8))
    assert bal[D(2026, 5, 5)] == 108.96373
    assert bal[D(2026, 5, 6)] == 108.88629
    assert bal[D(2026, 5, 8)] == 108.88629  # carried forward to today


def test_real_kinesis_holdings_sum_available_and_allocated():
    holdings = {
        "KAU": {"available": 106.0475457584, "allocatedOnExchange": 8.46e-08},
        "KAG": {"available": 157.4727742986, "allocatedOnExchange": 2.016e-05},
        "C1USD": {"available": 69.2675908397, "allocatedOnExchange": 0},
        "BTC": {"available": 0, "allocatedOnExchange": 0},
    }
    parsed = assets.parse_holdings(holdings)
    assert parsed["KAU"] == 106.0475457584 + 8.46e-08
    assert parsed["KAG"] == 157.4727742986 + 2.016e-05
    assert parsed["C1USD"] == 69.2675908397
    assert parsed["BTC"] == 0


def test_usd_pegged():
    assert assets.is_usd_pegged("C1USD")
    assert assets.is_usd_pegged("USD")
    assert not assets.is_usd_pegged("KAU")


def test_real_kinesis_date_with_utc_suffix():
    assert history.parse_date("2026-05-05 16:22:31 UTC") == D(2026, 5, 5)


# ---- KVT: no transaction history -> constant-holdings price series ----------


class _FakeClient:
    """Minimal stand-in for KinesisClient (no network) for build_symbol_result."""

    def __init__(self, ohlc):
        self._ohlc = ohlc

    def get_ohlc(self, pair, frm, to, tf):
        return self._ohlc

    def get_mid_price(self, pair):  # not used when history rows exist
        return {"mid_price": 99.0}


def test_kvt_constant_holdings_value_series():
    from types import SimpleNamespace

    cfg = SimpleNamespace(kinesis_quote="C1USD", ohlc_timeframe="1440")
    ohlc = [
        {"date": "2026-05-05T00:00:00Z", "close": 2.0},
        {"date": "2026-05-06T00:00:00Z", "close": 3.0},
    ]
    client = _FakeClient(ohlc)
    holdings = {"KVT": 50.0}
    events = []  # KVT absent from the statement (held, not traded)

    res = connector.build_symbol_result(
        client, cfg, "KVT", holdings, events,
        overall_start=D(2026, 5, 5), assume_constant=True,
    )
    assert res.account_name == "Kinesis Velocity Token (KVT)"
    # Value = constant 50 KVT x daily close.
    assert res.history_rows[0] == (D(2026, 5, 5), 100.0)
    assert res.history_rows[1] == (D(2026, 5, 6), 150.0)
    # Forward-filled afterward at the last close (3.0) -> 150.0 to today.
    assert res.history_rows[-1][1] == 150.0
    assert res.current_value == 150.0
    assert "constant current holdings" in res.note


def test_no_history_no_overall_start_sets_current_only():
    from types import SimpleNamespace

    cfg = SimpleNamespace(kinesis_quote="C1USD", ohlc_timeframe="1440")
    client = _FakeClient([])
    res = connector.build_symbol_result(
        client, cfg, "KVT", {"KVT": 50.0}, [], overall_start=None
    )
    assert res.history_rows == []
    # Falls back to live mid-price for current value: 50 * 99.0
    assert res.current_value == 4950.0
