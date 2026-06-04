"""Command-line entrypoint for the Kinesis -> Monarch connector."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Optional

from . import connector, monarch_sync
from .config import Config, ConfigError
from .kinesis import KinesisClient, KinesisError


def _parse_symbols(value: Optional[str]) -> Optional[list[str]]:
    if not value:
        return None
    return [s.strip().upper() for s in value.split(",") if s.strip()]


# ---- commands --------------------------------------------------------------


def cmd_inspect(config: Config, args) -> int:
    """Dump raw Kinesis API responses so JSON shapes can be verified."""
    client = KinesisClient(
        config.kinesis_public_key, config.kinesis_private_key, config.kinesis_base_url
    )
    os.makedirs(args.out, exist_ok=True)

    def dump(name: str, fn):
        try:
            data = fn()
        except KinesisError as exc:
            print(f"  {name}: ERROR {exc}")
            return None
        path = os.path.join(args.out, f"{name}.json")
        with open(path, "w") as fh:
            json.dump(data, fh, indent=2, default=str)
        preview = json.dumps(data, default=str)
        print(f"  {name}: ok -> {path}")
        print(f"    {preview[:300]}{'...' if len(preview) > 300 else ''}")
        return data

    print("Fetching raw Kinesis responses:")
    dump("holdings", client.get_holdings)
    dump("pairs", client.get_pairs)
    dump("balance_statement", client.get_balance_statement)
    # A sample OHLC pull (last ~30 days) to confirm candle shape + timeFrame unit.
    import datetime as dt

    end = connector.today_utc()
    start = end - dt.timedelta(days=30)
    pair = f"KAU_{config.kinesis_quote}"

    # Probe several timeFrame values to discover which unit returns candles
    # (the Kinesis docs are ambiguous about minutes vs seconds).
    print(f"Probing OHLC for {pair} across timeframes:")
    first_nonempty = None
    for tf in ["1440", "86400", "60", "3600", "1", "D", "1D"]:
        try:
            data = client.get_ohlc(
                pair, connector._iso(start), connector._iso(end, end_of_day=True), tf
            )
            n = len(data) if isinstance(data, list) else "n/a"
            print(f"    timeFrame={tf!r}: {n} candles")
            if isinstance(data, list) and data and first_nonempty is None:
                first_nonempty = (tf, data)
        except KinesisError as exc:
            print(f"    timeFrame={tf!r}: ERROR {exc}")
    if first_nonempty:
        tf, data = first_nonempty
        path = os.path.join(args.out, "ohlc_KAU_sample.json")
        with open(path, "w") as fh:
            json.dump(data, fh, indent=2, default=str)
        print(f"  -> non-empty at timeFrame={tf!r}; saved sample to {path}")
        print(f"     first candle: {json.dumps(data[0], default=str)[:300]}")
        print(f"     (set OHLC_TIMEFRAME={tf} in .env if it isn't already)")
    else:
        print("  -> no timeframe returned candles; share this output.")

    dump("mid_price_KAU", lambda: client.get_mid_price(pair))
    print(f"\nRaw JSON written to {args.out}/. Share these if parsing looks off.")
    return 0


def cmd_probe(config: Config, args) -> int:
    """Discover how to fetch the FULL balance statement (not just ~30 days)."""
    import datetime as dt

    client = KinesisClient(
        config.kinesis_public_key, config.kinesis_private_key, config.kinesis_base_url
    )
    now = connector._iso(connector.today_utc(), end_of_day=True)
    early = "2017-01-01T00:00:00.000Z"  # before Kinesis launched
    variants = [
        ("baseline (no params)", None),
        ("fromDate/toDate", {"fromDate": early, "toDate": now}),
        ("from/to", {"from": early, "to": now}),
        ("startDate/endDate", {"startDate": early, "endDate": now}),
        ("dateFrom/dateTo", {"dateFrom": early, "dateTo": now}),
        ("limit", {"limit": 100000}),
        ("pageSize", {"pageSize": 100000}),
        ("page/pageSize", {"page": 1, "pageSize": 100000}),
        ("offset/limit", {"offset": 0, "limit": 100000}),
    ]

    def summarize(data):
        if not isinstance(data, list):
            return f"non-list response: {str(data)[:120]}"
        dates = sorted(
            e.get("DateTime", "") for e in data if isinstance(e, dict) and e.get("DateTime")
        )
        span = f"{dates[0]} -> {dates[-1]}" if dates else "no dates"
        kvt = sum(
            1 for e in data if isinstance(e, dict) and e.get("Currency_Code") == "KVT"
        )
        return f"{len(data):>5} entries | {span} | KVT rows: {kvt}"

    print("Probing account-balance-statement for full history:\n")
    best = None
    for name, params in variants:
        try:
            data = client.get_balance_statement(params=params)
        except KinesisError as exc:
            print(f"  {name:22}: ERROR {exc}")
            continue
        line = summarize(data)
        print(f"  {name:22}: {line}")
        if isinstance(data, list) and (best is None or len(data) > best[2]):
            best = (name, params, len(data))

    if best and best[2] > 55:
        print(f"\n  -> '{best[0]}' returned the most ({best[2]} entries). That's the param to use.")
        os.makedirs(args.out, exist_ok=True)
        full = client.get_balance_statement(params=best[1])
        path = os.path.join(args.out, "balance_statement_full.json")
        with open(path, "w") as fh:
            json.dump(full, fh, indent=2, default=str)
        print(f"     full statement saved to {path}")
    else:
        print("\n  -> no variant returned more than the baseline; share this output.")
    return 0


def _print_results(results) -> None:
    for r in results:
        span = ""
        if r.history_rows:
            span = f" | history {r.history_rows[0][0]} -> {r.history_rows[-1][0]} ({len(r.history_rows)} days)"
        val = f"${r.current_value:,.2f}" if r.current_value is not None else "n/a"
        print(f"  {r.account_name}: {r.current_amount:g} {r.symbol} = {val}{span}")
        if r.note:
            print(f"    note: {r.note}")


def cmd_preview(config: Config, args) -> int:
    """Build value histories and write CSVs locally; no Monarch login."""
    results = connector.gather(config, _parse_symbols(args.symbols), args.all)
    print("Built value series (no Monarch login):")
    _print_results(results)
    os.makedirs(args.out, exist_ok=True)
    written = []
    for r in results:
        path = os.path.join(args.out, f"{r.symbol}.csv")
        with open(path, "w") as fh:
            fh.write(r.csv)
        written.append((r.account_name, path))
        print(f"  wrote {path}")

    print("\nNext — import into Monarch yourself (password-free, one-time):")
    print("  1. In Monarch, create a manual ASSET account for each, named exactly:")
    for name, _ in written:
        print(f"       - {name}")
    print("  2. For each account, use Monarch's \"Import Balance History\" and upload:")
    for name, path in written:
        print(f"       - {path}   ->  {name}")
    print("     (Monarch help: search \"Importing Account Balance History Manually\".)")
    return 0


async def _write_to_monarch(config: Config, results, upload_history: bool) -> None:
    mm = await monarch_sync.login(config)
    print("Logged in to Monarch.")
    for r in results:
        if r.current_value is None:
            print(f"  {r.account_name}: skipped (no value computed)")
            continue
        account_id = await monarch_sync.ensure_account(
            mm, r.account_name, initial_balance=r.current_value
        )
        if upload_history and r.history_rows:
            await monarch_sync.upload_history(mm, account_id, r.csv)
            print(
                f"  {r.account_name}: uploaded {len(r.history_rows)} days of history "
                f"+ set balance ${r.current_value:,.2f}"
            )
        await monarch_sync.set_balance(mm, account_id, r.current_value)
        if not (upload_history and r.history_rows):
            print(f"  {r.account_name}: set balance ${r.current_value:,.2f}")


_BLOCKED_MSG = """
Monarch rejected the automated login (likely HTTP 429). Monarch actively blocks
the unofficial library's login and steers automated access to its OAuth MCP
(which cannot write balances/history). This path also violates Monarch's ToS.

Use the password-free flow instead:
  1. .venv/bin/python sync.py preview --symbols KAU,KAG,KVT
  2. In Monarch, create the manual accounts and import the export/*.csv files
     via Monarch's "Import Balance History" feature.
"""


def _is_monarch_blocked(exc: Exception) -> bool:
    return "429" in str(exc) or type(exc).__name__ == "LoginFailedException"


def _run_monarch_write(config: Config, results, upload_history: bool) -> int:
    try:
        asyncio.run(_write_to_monarch(config, results, upload_history=upload_history))
    except Exception as exc:  # noqa: BLE001 - surface a helpful message, not a trace
        if _is_monarch_blocked(exc):
            print(_BLOCKED_MSG, file=sys.stderr)
            return 1
        raise
    return 0


def cmd_backfill(config: Config, args) -> int:
    config.require_monarch()
    results = connector.gather(config, _parse_symbols(args.symbols), args.all)
    print("Computed value series:")
    _print_results(results)
    if args.dry_run:
        print("\n--dry-run: not writing to Monarch.")
        return 0
    print("\nNOTE: automated Monarch push is blocked by Monarch; preview + UI import is the supported path.")
    rc = _run_monarch_write(config, results, upload_history=True)
    if rc == 0:
        print("\nDone. Check Monarch — the accounts now show value over time.")
    return rc


def cmd_update(config: Config, args) -> int:
    config.require_monarch()
    results = connector.gather(config, _parse_symbols(args.symbols), args.all)
    print("Current values:")
    _print_results(results)
    if args.dry_run:
        print("\n--dry-run: not writing to Monarch.")
        return 0
    rc = _run_monarch_write(config, results, upload_history=False)
    if rc == 0:
        print("\nDone. Balances updated.")
    return rc


# ---- parser ----------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="kinesis-monarch",
        description="Import Kinesis Money holdings (with history) into Monarch Money.",
    )
    sub = p.add_subparsers(dest="command", required=True)

    def add_common(sp, out_default="debug"):
        sp.add_argument("--symbols", help="Comma-separated symbols, e.g. KAU,KAG,KVT. Default: SYMBOLS env, else metals.")
        sp.add_argument("--all", action="store_true", help="Include every held symbol, not just metals.")
        sp.add_argument("--out", default=out_default, help=f"Output directory for files (default: {out_default}/).")

    sp = sub.add_parser("inspect", help="Dump raw Kinesis responses to verify shapes.")
    sp.add_argument("--out", default="debug")
    sp.set_defaults(func=cmd_inspect)

    sp = sub.add_parser("probe", help="Discover params to fetch FULL statement history.")
    sp.add_argument("--out", default="debug")
    sp.set_defaults(func=cmd_probe)

    sp = sub.add_parser("preview", help="Build value series + CSVs locally; no Monarch login.")
    add_common(sp, out_default="export")
    sp.set_defaults(func=cmd_preview)

    sp = sub.add_parser("backfill", help="Create accounts + upload full history to Monarch.")
    add_common(sp)
    sp.add_argument("--dry-run", action="store_true")
    sp.set_defaults(func=cmd_backfill)

    sp = sub.add_parser("update", help="Update current balances in Monarch (no history).")
    add_common(sp)
    sp.add_argument("--dry-run", action="store_true")
    sp.set_defaults(func=cmd_update)

    return p


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = Config.load()
    except ConfigError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 2
    try:
        return args.func(config, args)
    except (KinesisError, ConfigError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
